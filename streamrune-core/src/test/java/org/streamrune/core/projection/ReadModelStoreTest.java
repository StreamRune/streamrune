package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ReadModelStoreTest {

  private static final ProjectionRepository REPOSITORY =
      (ProjectionRepository)
          Proxy.newProxyInstance(
              ReadModelStoreTest.class.getClassLoader(),
              new Class<?>[] {ProjectionRepository.class},
              (proxy, method, args) -> {
                throw new UnsupportedOperationException(method.getName());
              });

  private static final OffsetStore CHECKPOINTS =
      new OffsetStore() {
        @Override
        public GlobalOffset getLastOffset(ProjectionName projectionName) {
          return GlobalOffset.initial();
        }

        @Override
        public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
          // not used
        }
      };

  private static final AtomicBatchProcessor PROCESSOR =
      (name, batch, newOffset, epoch, updater, offsetStore) -> updater.update(null);

  @Test
  void ofRejectsNullParts() {
    assertThrows(
        NullPointerException.class,
        () -> ReadModelStore.of(null, REPOSITORY, CHECKPOINTS, PROCESSOR));
    assertThrows(
        NullPointerException.class,
        () -> ReadModelStore.of("postgres", null, CHECKPOINTS, PROCESSOR));
    assertThrows(
        NullPointerException.class,
        () -> ReadModelStore.of("postgres", REPOSITORY, null, PROCESSOR));
    assertThrows(
        NullPointerException.class,
        () -> ReadModelStore.of("postgres", REPOSITORY, CHECKPOINTS, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> ReadModelStore.of(" ", REPOSITORY, CHECKPOINTS, PROCESSOR));
    assertThrows(
        IllegalArgumentException.class,
        () -> ReadModelStore.of("", REPOSITORY, CHECKPOINTS, PROCESSOR));
  }

  @Test
  void ofExposesItsParts() {
    var store = ReadModelStore.of("postgres", REPOSITORY, CHECKPOINTS, PROCESSOR);
    assertEquals("postgres", store.name());
    assertSame(REPOSITORY, store.repository());
    assertSame(CHECKPOINTS, store.checkpoints());
    assertSame(PROCESSOR, store.processor());
  }

  @Test
  void checkpointTargetPairsTheStoresCheckpointsAndProcessor() {
    var store = ReadModelStore.of("postgres", REPOSITORY, CHECKPOINTS, PROCESSOR);
    var target = store.checkpointTarget();
    assertEquals("postgres", target.name());
    assertSame(store.checkpoints(), target.checkpoints());
    assertSame(store.processor(), target.processor());
  }

  @Test
  void checkpointTargetDefaultsFromAHandWrittenStore() {
    ReadModelStore store =
        new ReadModelStore() {
          @Override
          public String name() {
            return "search";
          }

          @Override
          public ProjectionRepository repository() {
            return REPOSITORY;
          }

          @Override
          public OffsetStore checkpoints() {
            return CHECKPOINTS;
          }

          @Override
          public AtomicBatchProcessor processor() {
            return PROCESSOR;
          }
        };
    assertEquals(new CheckpointTarget("search", CHECKPOINTS, PROCESSOR), store.checkpointTarget());
  }

  @Test
  void checkpointTargetRejectsNullPartsAndABlankName() {
    assertThrows(
        NullPointerException.class, () -> new CheckpointTarget(null, CHECKPOINTS, PROCESSOR));
    assertThrows(
        NullPointerException.class, () -> new CheckpointTarget("subscription", null, PROCESSOR));
    assertThrows(
        NullPointerException.class, () -> new CheckpointTarget("subscription", CHECKPOINTS, null));
    assertThrows(
        IllegalArgumentException.class, () -> new CheckpointTarget(" ", CHECKPOINTS, PROCESSOR));
  }

  @Test
  void nonAtomicAtLeastOnceIsNeitherTransactionalNorFencing() {
    var p = AtomicBatchProcessor.nonAtomicAtLeastOnce();
    assertFalse(p.transactional());
    assertFalse(p.fencesCheckpoint());
  }

  @Test
  void aProcessorDeclaresNeitherCapabilityByDefault() {
    assertFalse(PROCESSOR.transactional());
    assertFalse(PROCESSOR.fencesCheckpoint());
  }
}
