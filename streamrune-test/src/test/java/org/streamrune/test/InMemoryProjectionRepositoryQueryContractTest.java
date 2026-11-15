package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Sort;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;

class InMemoryProjectionRepositoryQueryContractTest extends ProjectionRepositoryQueryContract {

  private InMemoryProjectionRepository repo;

  @BeforeEach
  void fresh() {
    repo = new InMemoryProjectionRepository();
  }

  @Override
  protected ProjectionRepository repository() {
    return repo;
  }

  @Test
  void declaredIndexesAreRecordedOnceInDeclarationOrder() {
    var name = ProjectionName.of("recorded");
    var first = ProjectionIndex.forSort(name, Sort.by("b"));
    var second = ProjectionIndex.forFilter(name, "a", Sort.unsorted());

    repo.declareIndex(first);
    repo.declareIndex(second);
    repo.declareIndex(first);

    assertEquals(List.of(first, second), repo.declaredIndexes());
  }

  @Test
  void aNullFieldMapperIsRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new InMemoryProjectionRepository((ObjectMapper) null));
  }

  /** A read model the mapper cannot write is refused at save, as the JDBC repository refuses it. */
  @Test
  void aReadModelTheMapperCannotWriteIsRefusedAtSave() {
    var name = ProjectionName.of("unwritable");
    var unwritable = new Object();

    assertThrows(EventStoreException.class, () -> repo.save(name, "u1", unwritable));
    assertThrows(EventStoreException.class, () -> repo.save(name, "u1", unwritable, 0L));
    assertThrows(
        EventStoreException.class,
        () -> repo.executeReplay(name, List.of(), tx -> tx.save(name, "u1", unwritable)));
    assertEquals(0, repo.rowCount(name));
  }
}
