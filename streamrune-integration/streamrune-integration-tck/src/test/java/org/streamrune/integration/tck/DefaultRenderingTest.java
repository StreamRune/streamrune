package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DefaultRenderingTest {

  enum Replay {
    OPERATOR_ONLY
  }

  @Test
  void aDurationIsTheShortestExactUnit() {
    assertEquals("7d", DefaultRendering.render(Duration.ofDays(7)));
    assertEquals("7d", DefaultRendering.render(Duration.parse("PT168H")));
    assertEquals("30d", DefaultRendering.render(Duration.ofHours(720)));
    assertEquals("1h", DefaultRendering.render(Duration.ofMinutes(60)));
    assertEquals("1m", DefaultRendering.render(Duration.ofSeconds(60)));
    assertEquals("5m", DefaultRendering.render(Duration.ofMinutes(5)));
    assertEquals("90s", DefaultRendering.render(Duration.ofSeconds(90)));
    assertEquals("30s", DefaultRendering.render(Duration.ofSeconds(30)));
    assertEquals("500ms", DefaultRendering.render(Duration.ofMillis(500)));
    assertEquals("1500ms", DefaultRendering.render(Duration.ofMillis(1500)));
  }

  @Test
  void zeroNegativeAndSubMillisecondDurations() {
    assertEquals("0s", DefaultRendering.render(Duration.ZERO));
    assertEquals("-5s", DefaultRendering.render(Duration.ofSeconds(-5)));
    assertEquals("PT0.0005S", DefaultRendering.render(Duration.ofNanos(500_000)));
  }

  @Test
  void scalarsAreTheirText() {
    assertEquals("true", DefaultRendering.render(true));
    assertEquals("false", DefaultRendering.render(Boolean.FALSE));
    assertEquals("100", DefaultRendering.render(100));
    assertEquals("5000", DefaultRendering.render(5000L));
    assertEquals("2.0", DefaultRendering.render(2.0));
    assertEquals("auto", DefaultRendering.render("auto"));
    assertEquals("operator-only", DefaultRendering.render(Replay.OPERATOR_ONLY));
  }

  @Test
  void noValueIsTheDash() {
    assertEquals("—", DefaultRendering.NONE);
    assertEquals("—", DefaultRendering.render(null));
    assertEquals("—", DefaultRendering.render(Optional.empty()));
    assertEquals("—", DefaultRendering.render(""));
    assertEquals("—", DefaultRendering.render(List.of()));
  }

  @Test
  void anOptionalAndACollectionRenderTheirElements() {
    assertEquals("5s", DefaultRendering.render(Optional.of(Duration.ofSeconds(5))));
    assertEquals("role, tenant", DefaultRendering.render(List.of("role", "tenant")));
  }
}
