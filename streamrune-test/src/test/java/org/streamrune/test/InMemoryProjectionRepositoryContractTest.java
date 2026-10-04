package org.streamrune.test;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class InMemoryProjectionRepositoryContractTest extends AtomicBatchProcessorContract {

  private InMemoryProjectionRepository repo;

  @BeforeEach
  void fresh() {
    repo = new InMemoryProjectionRepository();
  }

  @Override
  protected AtomicBatchProcessor processor() {
    return repo;
  }

  @Override
  protected ProjectionRepository repositoryOfAnotherStore() {
    return new InMemoryProjectionRepository();
  }

  @Override
  protected OffsetStore offsetStore() {
    return repo;
  }

  @Override
  protected GlobalOffset committedOffset(ProjectionName name) {
    return repo.committedOffset(name);
  }

  @Override
  protected Optional<ContractView> readFromOutside(ProjectionName name, String id) {
    return repo.findById(name, id, ContractView.class);
  }
}
