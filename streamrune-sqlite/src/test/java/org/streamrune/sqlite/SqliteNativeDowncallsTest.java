package org.streamrune.sqlite;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The guard's downcall descriptors in the form the integrations' reachability metadata registers
 * them; the integrations' parity tests compare their metadata with this list.
 */
class SqliteNativeDowncallsTest {

  @Test
  void theGuardsFourDowncallsInReachabilityMetadataForm() {
    assertThat(SqliteNativeDowncalls.reachabilityMetadata())
        .isEqualTo(
            List.of(
                "jint [void*, jint, jint] {\"firstVariadicArg\":2,\"captureCallState\":true}",
                "jint [jint, jint] {\"captureCallState\":true}",
                "jint [jint] {\"captureCallState\":true}",
                "void* [jint] {}"));
  }

  @Test
  void theOptionsTheGuardBindsWith() {
    assertThat(SqliteNativeDowncalls.OPEN.options()).hasSize(2);
    assertThat(SqliteNativeDowncalls.FLOCK.options()).hasSize(1);
    assertThat(SqliteNativeDowncalls.STRERROR.options()).isEmpty();
  }

  @Test
  void aLayoutWithoutACanonicalNameIsRefused() {
    assertThatThrownBy(() -> SqliteNativeDowncalls.canonical(JAVA_LONG))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("no canonical layout name for ");
  }
}
