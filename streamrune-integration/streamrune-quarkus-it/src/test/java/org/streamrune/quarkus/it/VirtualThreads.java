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
   * Whether a thread of this name is one the framework starts: a projection runner, a retention
   * sweeper, the outbox poller, a saga sweeper or timeout runner, a subscription's poll thread, the
   * lease heartbeat, the dead-letter retry runner or the SSE feed.
   *
   * @param name the thread's name
   * @return whether the framework names its threads so
   */
  static boolean isFramework(String name) {
    return name.startsWith("streamrune-")
        || name.startsWith("projection-")
        || name.startsWith("scheduled-projection-")
        || name.startsWith("saga-")
        || name.endsWith("-retention-sweeper")
        || name.equals("outbox-poller");
  }

  /**
   * The virtual threads alive now.
   *
   * @return their names by thread id
   * @throws IOException if the dump cannot be written or read
   */
  static Map<Long, String> alive() throws IOException {
    Map<Long, String> threads = new HashMap<>();
    for (JsonNode thread : dump()) {
      threads.put(thread.path("tid").asLong(), thread.path("name").asText());
    }
    return threads;
  }

  /**
   * The stacks of those of {@code tids} that are alive now, one thread after the other, for a
   * failure message.
   *
   * @param tids the thread ids
   * @return the name and the stack of each
   * @throws IOException if the dump cannot be written or read
   */
  static String stacks(java.util.Set<Long> tids) throws IOException {
    StringBuilder out = new StringBuilder();
    for (JsonNode thread : dump()) {
      if (tids.contains(thread.path("tid").asLong())) {
        out.append('\n').append(thread.path("name").asText()).append(':');
        thread.path("stack").forEach(frame -> out.append("\n  at ").append(frame.asText()));
      }
    }
    return out.toString();
  }

  /** The virtual threads of a JSON thread dump taken now. */
  private static java.util.List<JsonNode> dump() throws IOException {
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
    java.util.List<JsonNode> threads = new java.util.ArrayList<>();
    for (JsonNode container : dump.path("threadDump").path("threadContainers")) {
      for (JsonNode thread : container.path("threads")) {
        if (thread.path("virtual").asBoolean(false)) {
          threads.add(thread);
        }
      }
    }
    return threads;
  }
}
