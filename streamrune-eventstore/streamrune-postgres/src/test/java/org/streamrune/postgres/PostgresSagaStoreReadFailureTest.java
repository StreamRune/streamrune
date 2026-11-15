package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.types.SagaType;

/**
 * Every failure of a saga-row read reaches the caller as {@link EventStoreException}, the storage
 * failure the runner and the dead-letter stores handle: a full load and the read of the row's facts
 * alike, whether the driver throws a checked or an unchecked exception.
 */
class PostgresSagaStoreReadFailureTest {

  private static final SagaId ID = SagaId.of("read-failure-1");
  private static final SagaType TYPE = SagaType.of("ReadFailure");

  /** A data source whose every connection request fails with an unchecked exception. */
  private static DataSource failing() {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if ("getConnection".equals(method.getName())) {
                throw new IllegalStateException("the pool is closed");
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  @Test
  void loadRowFacts_wrapsAnUncheckedReadFailure_asLoadDoes() {
    var store = new PostgresSagaStore(failing(), new ObjectMapper());

    assertThatThrownBy(() -> store.load(ID, TYPE, SagaState.class))
        .isInstanceOf(EventStoreException.class)
        .hasRootCauseInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> store.loadRowFacts(ID, TYPE))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("read-failure-1")
        .hasRootCauseInstanceOf(IllegalStateException.class);
  }
}
