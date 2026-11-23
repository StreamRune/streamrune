package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ReadModelStoreDefaultAliasTest {

  private static final ProjectionRepository REPOSITORY =
      (ProjectionRepository)
          Proxy.newProxyInstance(
              ReadModelStoreDefaultAliasTest.class.getClassLoader(),
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
  void defaultAliasIsTheWordDefault() {
    assertThat(ReadModelStore.DEFAULT).isEqualTo("default");
  }

  @Test
  void ofRejectsTheReservedDefaultName() {
    assertThatThrownBy(
            () -> ReadModelStore.of(ReadModelStore.DEFAULT, REPOSITORY, CHECKPOINTS, PROCESSOR))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("'default' is reserved: it names the backend's default read-model store");
  }

  @Test
  void ofAcceptsANameThatOnlyContainsDefault() {
    ReadModelStore store = ReadModelStore.of("default-search", REPOSITORY, CHECKPOINTS, PROCESSOR);

    assertThat(store.name()).isEqualTo("default-search");
  }
}
