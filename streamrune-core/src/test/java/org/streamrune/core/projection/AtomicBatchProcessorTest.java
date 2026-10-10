package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class AtomicBatchProcessorTest {

  private static final class RecordingOffsetStore implements OffsetStore {
    final Map<ProjectionName, GlobalOffset> saved = new HashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return saved.getOrDefault(projectionName, GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      saved.put(projectionName, offset);
    }
  }

  /**
   * The default registration-time name check accepts every name — the nonatomic processor and the
   * in-memory processors key everything by the {@code ProjectionName} value alone. Only a processor
   * with a naming rule (the JDBC repository's table names) overrides it.
   */
  @Test
  void defaultCheckProjectionName_acceptsEveryName() {
    AtomicBatchProcessor custom = (pn, batch, newOffset, epoch, updater, offsetStore) -> {};
    for (String name :
        List.of("orders", "Orders", "order-summary", "order summary", "x".repeat(200))) {
      assertDoesNotThrow(
          () ->
              AtomicBatchProcessor.nonAtomicAtLeastOnce()
                  .checkProjectionName(ProjectionName.of(name)));
      assertDoesNotThrow(() -> custom.checkProjectionName(ProjectionName.of(name)));
    }
  }

  @Test
  void nonAtomicAtLeastOnce_handsNullToTheUpdater_thenSavesTheOffset() {
    var store = new RecordingOffsetStore();
    var handed = new ArrayList<ProjectionRepository>();
    var order = new ArrayList<String>();
    AtomicBatchProcessor.nonAtomicAtLeastOnce()
        .executeAtomically(
            ProjectionName.of("orders"),
            List.of(),
            GlobalOffset.of(7),
            0L,
            repo -> {
              handed.add(repo);
              order.add("update:" + store.saved.containsKey(ProjectionName.of("orders")));
            },
            store);
    assertEquals(1, handed.size());
    assertNull(handed.get(0), "the nonatomic processor has no transaction-scoped repository");
    assertEquals(List.of("update:false"), order, "the save happens AFTER the updater returned");
    assertEquals(GlobalOffset.of(7), store.saved.get(ProjectionName.of("orders")));
  }

  @Test
  void nonAtomicAtLeastOnce_refusesANonZeroEpoch_beforeTheUpdaterRuns() {
    var store = new RecordingOffsetStore();
    var updaterRan = new AtomicBoolean();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                AtomicBatchProcessor.nonAtomicAtLeastOnce()
                    .executeAtomically(
                        ProjectionName.of("orders"),
                        List.of(),
                        GlobalOffset.of(7),
                        3L,
                        repo -> updaterRan.set(true),
                        store));
    assertTrue(ex.getMessage().contains("nonAtomicAtLeastOnce"), ex.getMessage());
    assertTrue(ex.getMessage().contains("epoch 3"), ex.getMessage());
    assertFalse(updaterRan.get());
    assertTrue(store.saved.isEmpty());
  }

  @Test
  void nonAtomicAtLeastOnce_wrapsASaveFailure_inProjectionCheckpointSaveException() {
    var boom = new IllegalStateException("offset store down");
    OffsetStore failing =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName projectionName) {
            return GlobalOffset.initial();
          }

          @Override
          public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
            throw boom;
          }
        };
    var updaterRuns = new AtomicInteger();
    var ex =
        assertThrows(
            ProjectionCheckpointSaveException.class,
            () ->
                AtomicBatchProcessor.nonAtomicAtLeastOnce()
                    .executeAtomically(
                        ProjectionName.of("orders"),
                        List.of(),
                        GlobalOffset.of(9),
                        0L,
                        repo -> updaterRuns.incrementAndGet(),
                        failing));
    assertEquals(1, updaterRuns.get(), "the batch WAS applied — that is the state the type names");
    assertEquals(ProjectionName.of("orders"), ex.projectionName());
    assertEquals(GlobalOffset.of(9), ex.offset());
    assertSame(boom, ex.getCause());
  }

  @Test
  void nonAtomicAtLeastOnce_doesNotWrapAnUpdaterFailure() {
    var store = new RecordingOffsetStore();
    var boom = new IllegalStateException("projection bug");
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                AtomicBatchProcessor.nonAtomicAtLeastOnce()
                    .executeAtomically(
                        ProjectionName.of("orders"),
                        List.of(),
                        GlobalOffset.of(9),
                        0L,
                        repo -> {
                          throw boom;
                        },
                        store));
    assertSame(boom, ex);
    assertTrue(store.saved.isEmpty(), "a failed updater never advances the checkpoint");
  }

  @Test
  void nonAtomicAtLeastOnce_isOneNamedInstance_thatCannotFence() {
    var p = AtomicBatchProcessor.nonAtomicAtLeastOnce();
    assertSame(p, AtomicBatchProcessor.nonAtomicAtLeastOnce());
    assertInstanceOf(AtomicBatchProcessor.NonAtomicAtLeastOnce.class, p);
    assertEquals("nonAtomicAtLeastOnce", p.toString());
    assertFalse(p.supportsFencing());
    assertThrows(
        UnsupportedOperationException.class,
        () -> p.stampFencingEpoch(ProjectionName.of("orders"), 1L));
  }

  /** A processor that is also a repository, with an explicit identity object. */
  private static ProjectionRepository repositoryWithIdentity(Object identity) {
    return (ProjectionRepository)
        Proxy.newProxyInstance(
            AtomicBatchProcessorTest.class.getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (proxy, method, args) -> {
              if (method.getName().equals("writeTargetIdentity")) {
                return identity;
              }
              if (method.getName().equals("writesTo")) {
                // A JDK proxy cannot run the interface default itself; forward to the default via
                // InvocationHandler.invokeDefault, exactly as a bean proxy forwards to its target.
                return InvocationHandler.invokeDefault(proxy, method, args);
              }
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "toString" -> "proxy(" + identity + ")";
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "equals" -> proxy == args[0];
                  default -> throw new UnsupportedOperationException(method.getName());
                };
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  @Test
  void writesTo_default_comparesWriteTargetIdentities_notReferences() {
    var a1 = (AtomicBatchProcessor) repositoryWithIdentity("store-A");
    var a2 = repositoryWithIdentity("store-A");
    var b = repositoryWithIdentity("store-B");
    assertTrue(a1.writesTo(a2), "equal identities ⇒ same store, even across proxies");
    assertFalse(a1.writesTo(b));
    assertTrue(a1.writesTo((ProjectionRepository) a1));
  }

  @Test
  void writesTo_default_forAProcessorThatIsNotARepository_usesTheProcessorItself() {
    AtomicBatchProcessor lambdaProcessor = (n, b, o, e, u, s) -> {};
    var target = repositoryWithIdentity(lambdaProcessor);
    assertTrue(lambdaProcessor.writesTo(target));
    assertFalse(lambdaProcessor.writesTo(repositoryWithIdentity("elsewhere")));
  }

  @Test
  void writesTo_requiresATarget() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AtomicBatchProcessor.nonAtomicAtLeastOnce().writesTo(null));
  }

  /**
   * A processor that has not implemented a per-projection lock does not claim one, and refuses to
   * replay a dead-lettered range beside a live runner: run unlocked, the replay and a live batch
   * can both read a row before either saves it. The refusal names the processor and the way out.
   */
  @Test
  void replayDefaults_claimNoLock_andRefuseTheReplay() {
    AtomicBatchProcessor custom = (pn, batch, newOffset, epoch, updater, offsetStore) -> {};
    var name = ProjectionName.of("orders\r\nforged");
    var updaterRan = new AtomicBoolean();

    for (AtomicBatchProcessor processor :
        List.of(AtomicBatchProcessor.nonAtomicAtLeastOnce(), custom)) {
      assertFalse(processor.serializesReplay());
      var refusal =
          assertThrows(
              UnsupportedOperationException.class,
              () -> processor.executeReplay(name, List.of(), repo -> updaterRan.set(true)));
      assertTrue(refusal.getMessage().contains("replayWithRunnerStopped"), refusal.getMessage());
      assertTrue(refusal.getMessage().contains("serializesReplay()"), refusal.getMessage());
      assertFalse(refusal.getMessage().contains("\r"), "the name is sanitized in the message");
    }
    assertFalse(updaterRan.get(), "a refused replay never runs the updater");
    assertTrue(
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                    AtomicBatchProcessor.nonAtomicAtLeastOnce()
                        .executeReplay(name, List.of(), repo -> {}))
            .getMessage()
            .contains("nonAtomicAtLeastOnce()"));
    assertTrue(
        assertThrows(
                UnsupportedOperationException.class,
                () -> custom.executeReplay(null, List.of(), repo -> {}))
            .getMessage()
            .contains("'null'"));
  }

  /** A processor without a structure per projection has nothing to prepare. */
  @Test
  void defaultPrepareReadModel_doesNothing() {
    AtomicBatchProcessor custom = (pn, batch, newOffset, epoch, updater, offsetStore) -> {};
    assertDoesNotThrow(() -> custom.prepareReadModel(ProjectionName.of("orders")));
    assertDoesNotThrow(() -> AtomicBatchProcessor.nonAtomicAtLeastOnce().prepareReadModel(null));
  }

  @Test
  void commitFencedException_carriesTheGuardThatRejected() {
    for (var guard : ProjectionCommitFencedException.Guard.values()) {
      var rejection = new ProjectionCommitFencedException(guard, "rejected");
      assertSame(guard, rejection.guard());
      assertEquals("rejected", rejection.getMessage());
    }
    assertThrows(
        IllegalArgumentException.class, () -> new ProjectionCommitFencedException(null, "x"));
  }

  /**
   * The default replay hook feeds the batch to the two-argument {@code process} with the repository
   * it was handed, and reports that it applied.
   */
  @Test
  void projectionReplayHook_default_processesThroughTheHandedRepository_andReportsApplied() {
    var handed = new ArrayList<ProjectionRepository>();
    var batches = new AtomicInteger();
    Projection projection =
        new Projection() {
          @Override
          public void process(List<org.streamrune.core.EventEnvelope> batch) {
            batches.incrementAndGet();
          }

          @Override
          public void process(
              List<org.streamrune.core.EventEnvelope> batch, ProjectionRepository repository) {
            handed.add(repository);
            process(batch);
          }
        };
    var repository = repositoryWithIdentity("store-A");

    assertTrue(projection.processDeadLetterReplay(List.of(), repository));
    assertTrue(projection.processDeadLetterReplay(List.of(), null));

    assertEquals(2, batches.get());
    assertEquals(2, handed.size());
    assertSame(repository, handed.get(0));
    assertNull(handed.get(1));
  }
}
