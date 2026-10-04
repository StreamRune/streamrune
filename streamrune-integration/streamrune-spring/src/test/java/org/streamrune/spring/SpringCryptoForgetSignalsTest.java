package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;

/**
 * The non-Postgres crypto backends must receive a prompt cross-replica forget channel when a
 * DataSource is present, not the NOOP that let a peer replica keep serving an erased subject's
 * cached plaintext until the write-TTL elapsed.
 */
class SpringCryptoForgetSignalsTest {

  @Test
  void resolvesToPostgresListenNotifyChannel_whenDataSourcePresent() {
    CryptoForgetSignal signal =
        SpringCryptoForgetSignals.resolve(mock(DataSource.class), Duration.ofMinutes(5), "aws-kms");
    assertInstanceOf(
        PostgresCryptoForgetSignal.class,
        signal,
        "a DataSource-backed backend must get the prompt LISTEN/NOTIFY forget channel, not NOOP");
  }

  @Test
  void fallsBackToNoop_whenNoDataSource() {
    CryptoForgetSignal signal = SpringCryptoForgetSignals.resolve(null, null, "vault");
    assertSame(
        CryptoForgetSignal.NOOP,
        signal,
        "with no DataSource the channel falls back to NOOP (peers converge via the write-TTL)");
  }
}
