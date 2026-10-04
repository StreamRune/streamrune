package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class CronExpressionTest {

  @Test
  void everySecondCron_nextFireOneSecondLater() {
    var cron = CronExpression.parse("* * * * * *", ZoneOffset.UTC);
    var from = Instant.parse("2026-01-01T00:00:00Z");
    var next = cron.nextFire(from);
    assertThat(next).isEqualTo(Instant.parse("2026-01-01T00:00:01Z"));
  }

  @Test
  void everyMinuteCron_nextFireAtNextMinute() {
    var cron = CronExpression.parse("0 * * * * *", ZoneOffset.UTC);
    var from = Instant.parse("2026-01-01T00:00:30Z");
    assertThat(cron.nextFire(from)).isEqualTo(Instant.parse("2026-01-01T00:01:00Z"));
  }

  @Test
  void invalidCron_throws() {
    assertThatThrownBy(() -> CronExpression.parse("not a cron", ZoneOffset.UTC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid cron");
  }

  @Test
  void blankCron_throws() {
    assertThatThrownBy(() -> CronExpression.parse("", ZoneOffset.UTC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blank");
  }

  @Test
  void nullCron_throws() {
    assertThatThrownBy(() -> CronExpression.parse(null, ZoneOffset.UTC))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
