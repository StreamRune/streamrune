package org.streamrune.test;

import org.streamrune.core.projection.OffsetStore;

/**
 * The offset-store contract against {@link InMemoryProjectionRepository}, the offset store of a
 * {@code TRANSACTIONAL_LOCAL} runner whose processor is the same repository.
 */
class InMemoryProjectionRepositoryOffsetStoreContractTest extends OffsetStoreContract {
  @Override
  protected OffsetStore newStore() {
    return new InMemoryProjectionRepository();
  }
}
