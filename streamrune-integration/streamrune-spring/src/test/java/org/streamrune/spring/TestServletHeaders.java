package org.streamrune.spring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/** Request-header doubles for {@code HttpServletRequest} mocks. */
final class TestServletHeaders {

  private TestServletHeaders() {}

  /**
   * The values of a request header ({@code X-User-Id}, {@code X-User-Role}) carrying one line per
   * non-null value, as {@code HttpServletRequest#getHeaders(String)} answers them.
   */
  static Enumeration<String> userIdHeader(String... values) {
    List<String> present = new ArrayList<>();
    for (String value : values) {
      if (value != null) {
        present.add(value);
      }
    }
    return Collections.enumeration(present);
  }
}
