package org.streamrune.quarkus;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;

/** Request-header doubles for JAX-RS {@code ContainerRequestContext} mocks. */
final class TestRequestHeaders {

  private TestRequestHeaders() {}

  /**
   * The multivalued headers of a request carrying one {@code X-User-Id} line per value, as {@code
   * ContainerRequestContext#getHeaders()} answers them; no {@code X-User-Id} when {@code values} is
   * empty or the single value is {@code null}.
   */
  static MultivaluedMap<String, String> userIdHeader(String... values) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    for (String value : values) {
      if (value != null) {
        headers.add("X-User-Id", value);
      }
    }
    return headers;
  }
}
