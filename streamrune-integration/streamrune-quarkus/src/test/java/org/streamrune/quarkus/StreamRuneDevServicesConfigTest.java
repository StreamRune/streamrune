package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;

import io.quarkus.runtime.StartupEvent;
import org.junit.jupiter.api.Test;

class StreamRuneDevServicesConfigTest {

  @Test
  void devServicesConfigClassExists() {
    assertThat(StreamRuneDevServicesConfig.class)
        .as("StreamRuneDevServicesConfig should exist")
        .isNotNull();
  }

  @Test
  void devServicesConfigHasStartupEventObserver() throws NoSuchMethodException {
    var method =
        StreamRuneDevServicesConfig.class.getDeclaredMethod("onStartup", StartupEvent.class);
    assertThat(method).isNotNull();
  }

  @Test
  void onStartupDoesNotThrow() {
    var config = new StreamRuneDevServicesConfig();
    assertDoesNotThrow(() -> config.onStartup(mock(StartupEvent.class)));
  }
}
