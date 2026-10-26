package org.streamrune.integration.tck;

import java.util.List;
import java.util.Map;

/**
 * An application of one integration as {@link IntegrationWiringContract} starts it: running, or
 * refused at startup. An integration's subclass of the contract implements it over the framework's
 * own container.
 */
public interface Application extends AutoCloseable {

  /**
   * Whether the application started.
   *
   * @return {@code false} when the integration refused the start
   */
  boolean started();

  /**
   * The refusal of a start that did not complete.
   *
   * @return the message of the exception the start failed with and the messages of its causes, one
   *     per line, outermost first; {@code null} when the application started
   */
  String startupFailureMessage();

  /**
   * A bean of the running application.
   *
   * @param type the bean type
   * @param <T> the bean type
   * @return the application's one bean of {@code type}
   */
  <T> T bean(Class<T> type);

  /**
   * Sends one request to the application's HTTP server.
   *
   * @param method the request method
   * @param path the path and query, starting with {@code /}
   * @param headers the request headers, one value each
   * @param body the request body, or {@code null} for none
   * @return the response as the client reads it; see {@link HttpResponse#body()} for a stream
   */
  HttpResponse http(String method, String path, Map<String, String> headers, String body);

  /**
   * The StreamRune health check as the application's health endpoint reports it, found under the
   * name {@link IntegrationWiringContract#healthCheckName()}.
   *
   * @return its status under {@code status} ({@code UP} or {@code DOWN}) and its details under
   *     {@code details}, keyed and nested as the endpoint writes them; an empty map when the
   *     endpoint reports no check of that name
   */
  Map<String, Object> health();

  /**
   * What the start logged at level {@code WARN} and above.
   *
   * @return one line per log record, its level, its logger's name and its formatted message, in the
   *     order logged
   */
  List<String> startupLogLines();

  /**
   * Shuts the application down the way its container does when the process ends; nothing for a
   * start that was refused.
   */
  @Override
  void close();
}
