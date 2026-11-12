package org.streamrune.crypto.filesystem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** The builder's metrics default to {@code StreamRuneMetrics.NOOP} and take no {@code null}. */
class FileSystemCryptoEngineBuilderMetricsTest {

  @Test
  void aNullMetricsIsRefused() {
    var refused =
        assertThrows(
            NullPointerException.class, () -> FileSystemCryptoEngine.builder().metrics(null));
    assertEquals("metrics is required", refused.getMessage());
  }
}
