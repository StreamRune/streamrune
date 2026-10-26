package org.streamrune.integration.tck;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * The HTTP client of the contract's adapters: one request to a server on {@code localhost}, its
 * response read the way {@link HttpResponse} describes. A {@code text/event-stream} response is
 * read up to its headers and its stream closed, so a stream that never ends does not hold the
 * client.
 */
public final class ContractHttp {

  /** How long a request may take before the client gives up. */
  static final Duration TIMEOUT = Duration.ofSeconds(30);

  private ContractHttp() {}

  /**
   * Sends one request.
   *
   * @param port the port of the server on {@code localhost}
   * @param method the request method
   * @param path the path and query, starting with {@code /}
   * @param headers the request headers, one value each
   * @param body the request body, or {@code null} for none
   * @return the response
   * @throws UncheckedIOException when the server cannot be reached or the response cannot be read
   */
  public static HttpResponse send(
      int port, String method, String path, Map<String, String> headers, String body) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(TIMEOUT)
            .method(
                method,
                body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    headers.forEach(request::header);
    try (HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(TIMEOUT)
            .build()) {
      java.net.http.HttpResponse<InputStream> response =
          client.send(request.build(), java.net.http.HttpResponse.BodyHandlers.ofInputStream());
      String text;
      try (InputStream in = response.body()) {
        boolean stream =
            response
                .headers()
                .firstValue("Content-Type")
                .map(type -> type.startsWith("text/event-stream"))
                .orElse(false);
        text = stream ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      return new HttpResponse(response.statusCode(), response.headers().map(), text);
    } catch (IOException e) {
      throw new UncheckedIOException(method + " " + path + " failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(method + " " + path + " was interrupted", e);
    }
  }
}
