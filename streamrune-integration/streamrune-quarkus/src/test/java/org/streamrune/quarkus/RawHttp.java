package org.streamrune.quarkus;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

/**
 * One HTTP/1.1 request written to a socket byte for byte, for what an HTTP client library does not
 * send: the same header on two lines, a header with an empty value.
 */
final class RawHttp {

  private RawHttp() {}

  /**
   * The answer to a request.
   *
   * @param status the status code of the status line
   * @param head the status line and the header lines, lower-cased
   * @param body the bytes after the blank line, as the server sent them (a chunked body keeps its
   *     chunk sizes)
   */
  record Answer(int status, String head, String body) {

    /** The value of the {@code Content-Type} header without spaces, lower-cased; "" when absent. */
    String contentType() {
      return head.lines()
          .filter(line -> line.startsWith("content-type:"))
          .map(line -> line.substring("content-type:".length()).replace(" ", ""))
          .findFirst()
          .orElse("");
    }
  }

  /**
   * Sends {@code POST path} with {@code headerLines} written exactly as given, each on a line of
   * its own and in order, and reads the answer until the server closes the connection.
   */
  static Answer post(int port, String path, List<String> headerLines) throws Exception {
    try (Socket socket = new Socket("localhost", port)) {
      socket.setSoTimeout(10_000);
      StringBuilder request = new StringBuilder();
      request.append("POST ").append(path).append(" HTTP/1.1\r\n");
      request.append("Host: localhost\r\n");
      request.append("Connection: close\r\n");
      request.append("Content-Length: 0\r\n");
      for (String line : headerLines) {
        request.append(line).append("\r\n");
      }
      request.append("\r\n");
      socket.getOutputStream().write(request.toString().getBytes(StandardCharsets.ISO_8859_1));
      socket.getOutputStream().flush();
      ByteArrayOutputStream received = new ByteArrayOutputStream();
      InputStream in = socket.getInputStream();
      byte[] chunk = new byte[4096];
      for (int read = in.read(chunk); read != -1; read = in.read(chunk)) {
        received.write(chunk, 0, read);
      }
      String response = received.toString(StandardCharsets.ISO_8859_1);
      int blank = response.indexOf("\r\n\r\n");
      String head = blank < 0 ? response : response.substring(0, blank);
      String body = blank < 0 ? "" : response.substring(blank + 4);
      int status = Integer.parseInt(head.split(" ", 3)[1]);
      return new Answer(status, head.toLowerCase(Locale.ROOT), body);
    }
  }
}
