package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * The adapter's native-image metadata for the HdrHistogram classes Micrometer builds behind a
 * distribution summary that publishes a percentile.
 *
 * <p>Micrometer's own metadata registers only the {@code (long, long, int)} constructors;
 * HdrHistogram 2.2 also copies a histogram through {@code ConcurrentHistogram(AbstractHistogram)},
 * which a native image refuses without a registration the first time the summary is read.
 */
class HistogramNativeMetadataTest {

  private static final String RESOURCE =
      "/META-INF/native-image/org.streamrune/streamrune-integration-api/reflect-config.json";

  @Test
  void theCopyAndSizingConstructorsAreRegistered() throws IOException {
    String json;
    try (InputStream in = getClass().getResourceAsStream(RESOURCE)) {
      assertThat(in).as(RESOURCE).isNotNull();
      json = new String(in.readAllBytes(), StandardCharsets.UTF_8).replaceAll("\\s+", " ");
    }

    for (String type : new String[] {"Histogram", "ConcurrentHistogram"}) {
      int start = json.indexOf("\"org.HdrHistogram." + type + "\"");
      assertThat(start).as(type).isNotNegative();
      int next = json.indexOf("{\"name\": \"org.HdrHistogram.", start + 1);
      String entry = json.substring(start, next < 0 ? json.length() : next);
      assertThat(entry)
          .as(type)
          .contains("\"parameterTypes\": [\"long\", \"long\", \"int\"]")
          .contains("\"parameterTypes\": [\"org.HdrHistogram.AbstractHistogram\"]");
    }
    assertThat(json).contains("\"org.HdrHistogram.DoubleHistogram\"");
  }

  @Test
  void theRegisteredConstructorsExist() throws ReflectiveOperationException {
    Class<?> abstractHistogram = Class.forName("org.HdrHistogram.AbstractHistogram");
    for (String type : new String[] {"Histogram", "ConcurrentHistogram"}) {
      Class<?> histogram = Class.forName("org.HdrHistogram." + type);
      assertThat(histogram.getConstructor(long.class, long.class, int.class)).isNotNull();
      assertThat(histogram.getConstructor(abstractHistogram)).isNotNull();
    }
  }
}
