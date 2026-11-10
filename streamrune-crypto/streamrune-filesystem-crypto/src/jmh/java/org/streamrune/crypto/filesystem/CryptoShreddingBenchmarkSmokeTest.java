package org.streamrune.crypto.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.CryptoState;

/**
 * Runs every {@link CryptoShreddingBenchmark} method twice against a real {@code @Setup} state,
 * outside the JMH harness. Part of {@code check} through {@code jmhSmokeTest}: a benchmark that
 * compiles but throws, or that serializes its fields in plaintext, fails the build.
 */
class CryptoShreddingBenchmarkSmokeTest {

  private final CryptoShreddingBenchmark benchmark = new CryptoShreddingBenchmark();

  @ParameterizedTest(name = "{0} encrypted field(s)")
  @ValueSource(ints = {1, 5})
  void serializeEncryptingRunsTwiceAndLeavesNoPlaintext(int encryptedFields) throws IOException {
    var state = new CryptoState();
    state.encryptedFields = encryptedFields;
    state.createEngine();
    try {
      benchmark.serializeEncrypting(state);
      String json = benchmark.serializeEncrypting(state);

      assertThat(json).contains("customer-42").contains("standard");
      assertThat(json).doesNotContain("jane.doe@example.com");
      assertThat(json).isNotEqualTo(state.json);
    } finally {
      state.deleteKeys();
    }
  }

  @ParameterizedTest(name = "{0} encrypted field(s)")
  @ValueSource(ints = {1, 5})
  void deserializeDecryptingRunsTwiceAndRestoresTheEvent(int encryptedFields) throws IOException {
    var state = new CryptoState();
    state.encryptedFields = encryptedFields;
    state.createEngine();
    try {
      benchmark.deserializeDecrypting(state);

      assertThat(benchmark.deserializeDecrypting(state)).isEqualTo(state.event);
    } finally {
      state.deleteKeys();
    }
  }
}
