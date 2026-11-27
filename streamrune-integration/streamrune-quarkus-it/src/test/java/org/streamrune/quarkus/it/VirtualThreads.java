package org.streamrune.quarkus.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** The virtual threads of this JVM, read from a thread dump, which lists them. */
final class VirtualThreads {

  private VirtualThreads() {}

  /**
   * The virtual threads alive now.
   *
   * @return their names by thread id
   * @throws IOException if the dump cannot be written or read
   */
  static Map<Long, String> alive() throws IOException {
    Path file = Files.createTempFile("streamrune-threads", ".json");
    JsonNode dump;
    try {
      // The dump refuses to overwrite a file.
      Files.delete(file);
      ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
          .dumpThreads(file.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON);
      dump = new ObjectMapper().readTree(file.toFile());
    } finally {
      Files.deleteIfExists(file);
    }
    Map<Long, String> threads = new HashMap<>();
    for (JsonNode container : dump.path("threadDump").path("threadContainers")) {
      for (JsonNode thread : container.path("threads")) {
        if (thread.path("virtual").asBoolean(false)) {
          threads.put(thread.path("tid").asLong(), thread.path("name").asText());
        }
      }
    }
    return threads;
  }
}
