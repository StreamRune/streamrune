package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LockPlatformTest {

  @ParameterizedTest
  @CsvSource({
    "Linux, LINUX, true",
    "Mac OS X, MACOS, true",
    "Windows 11, WINDOWS, false",
    "Windows Server 2022, WINDOWS, false"
  })
  void supportedSystemsGetTheirLock(String osName, LockPlatform expected, boolean flock) {
    LockPlatform platform = LockPlatform.of(osName);

    assertThat(platform).isEqualTo(expected);
    assertThat(platform.usesFlock()).isEqualTo(flock);
    assertThat(platform.flockConstants() != null).isEqualTo(flock);
  }

  @ParameterizedTest
  @ValueSource(strings = {"AIX", "FreeBSD", "OpenBSD", "SunOS", "z/OS", ""})
  void everyOtherSystemIsRefusedByName(String osName) {
    assertThatThrownBy(() -> LockPlatform.of(osName))
        .isInstanceOf(SqliteGuardException.class)
        .hasMessageContaining("'" + osName + "'")
        .hasMessageContaining("Linux, macOS and Windows");
  }

  @ParameterizedTest
  @CsvSource({"LINUX, 524288, 11", "MACOS, 16777216, 35"})
  void eachFlockSystemHasItsOwnConstants(LockPlatform platform, int oCloexec, int eWouldBlock) {
    assertThat(platform.flockConstants())
        .isEqualTo(new FlockFileLock.Constants(oCloexec, eWouldBlock));
  }
}
