package org.streamrune.test;

import org.junit.jupiter.api.BeforeEach;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;

class InMemoryProjectionRepositoryQueryContractTest extends ProjectionRepositoryContract {

  private InMemoryProjectionRepository repo;

  @BeforeEach
  void fresh() {
    repo = new InMemoryProjectionRepository();
  }

  @Override
  protected ProjectionRepository repository() {
    return repo;
  }

  @Override
  protected void declareIndex(ProjectionIndex index) {
    repo.declareIndex(index);
  }
}
