package org.streamrune.integration.tck;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * An HTTP response as a client of the application reads it.
 *
 * @param status the status code
 * @param headers the response headers, each name in lower case with its values in the order the
 *     client received them
 * @param body the body as UTF-8 text; empty for a {@code text/event-stream} response, whose stream
 *     the client closes once the status and the headers arrived
 */
public record HttpResponse(int status, Map<String, List<String>> headers, String body) {

  /** Copies the headers under lower-case names, so a lookup ignores the case a server sent. */
  public HttpResponse {
    Map<String, List<String>> lowerCase = new TreeMap<>();
    headers.forEach(
        (name, values) ->
            lowerCase
                .computeIfAbsent(name.toLowerCase(Locale.ROOT), _ -> new ArrayList<>())
                .addAll(values));
    lowerCase.replaceAll((name, values) -> List.copyOf(values));
    headers = Map.copyOf(lowerCase);
    body = body == null ? "" : body;
  }

  /**
   * The first value of a header.
   *
   * @param name the header name, in any case
   * @return its first value, or {@code null} when the response does not carry the header
   */
  public String header(String name) {
    List<String> values = headers.get(name.toLowerCase(Locale.ROOT));
    return values == null || values.isEmpty() ? null : values.getFirst();
  }
}
