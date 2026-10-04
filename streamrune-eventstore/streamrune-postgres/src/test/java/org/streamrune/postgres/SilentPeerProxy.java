package org.streamrune.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only TCP forwarder in front of PostgreSQL that can make the database host vanish without a
 * FIN or an RST.
 *
 * <p>{@link #silenceOpenConnections()} stops forwarding in both directions on every connection open
 * at that moment while keeping both sockets open: the client keeps an ESTABLISHED connection whose
 * writes succeed and whose reads never return data again. That is what a client sees when the
 * database host crashes, loses power or is replaced behind a virtual IP — no packet announces the
 * end of the connection, so only a timed read can notice it. The server keeps its session (and its
 * {@code LISTEN} registration), but everything it sends on that connection is dropped.
 *
 * <p>Connections opened after {@code silenceOpenConnections()} are forwarded normally, which models
 * a completed failover to a reachable primary. {@link #refuseNewConnections(boolean)} closes new
 * connections at accept instead, which models a primary that is not reachable yet.
 */
final class SilentPeerProxy implements AutoCloseable {

  private final ServerSocket server;
  private final String targetHost;
  private final int targetPort;
  private final List<Link> links = new CopyOnWriteArrayList<>();
  private final AtomicInteger forwardedConnections = new AtomicInteger();
  private volatile boolean refuseNew;
  private volatile boolean closed;

  SilentPeerProxy(String targetHost, int targetPort) throws IOException {
    this.targetHost = targetHost;
    this.targetPort = targetPort;
    this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    Thread.ofPlatform().daemon(true).name("silent-peer-proxy-accept").start(this::acceptLoop);
  }

  /** A JDBC URL that reaches the target database through this proxy. */
  String jdbcUrl(String database) {
    return "jdbc:postgresql://127.0.0.1:" + server.getLocalPort() + "/" + database;
  }

  /** Every connection open now stops answering in both directions; neither side is closed. */
  void silenceOpenConnections() {
    for (Link link : links) {
      link.silent = true;
    }
  }

  /** While {@code true}, a new connection is closed as soon as it is accepted. */
  void refuseNewConnections(boolean refuse) {
    this.refuseNew = refuse;
  }

  /** Connections accepted and forwarded to the target so far. */
  int forwardedConnections() {
    return forwardedConnections.get();
  }

  private void acceptLoop() {
    while (!closed) {
      Socket client;
      try {
        client = server.accept();
      } catch (IOException _) {
        return;
      }
      if (refuseNew) {
        closeQuietly(client);
        continue;
      }
      try {
        Socket upstream = new Socket(targetHost, targetPort);
        var link = new Link(client, upstream);
        links.add(link);
        forwardedConnections.incrementAndGet();
        Thread.ofVirtual().start(() -> pump(link, client, upstream));
        Thread.ofVirtual().start(() -> pump(link, upstream, client));
      } catch (IOException _) {
        closeQuietly(client);
      }
    }
  }

  private static void pump(Link link, Socket from, Socket to) {
    byte[] buffer = new byte[8192];
    try {
      InputStream in = from.getInputStream();
      OutputStream out = to.getOutputStream();
      int read;
      while ((read = in.read(buffer)) >= 0) {
        if (link.silent) {
          continue; // the bytes vanish; the connection stays open
        }
        out.write(buffer, 0, read);
        out.flush();
      }
    } catch (IOException _) {
      // one side went away
    }
    if (!link.silent) {
      link.close();
    }
  }

  @Override
  public void close() {
    closed = true;
    closeQuietly(server);
    for (Link link : links) {
      link.close();
    }
  }

  private static void closeQuietly(AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception _) {
      // best effort
    }
  }

  private static final class Link {
    private final Socket client;
    private final Socket upstream;
    volatile boolean silent;

    Link(Socket client, Socket upstream) {
      this.client = client;
      this.upstream = upstream;
    }

    void close() {
      closeQuietly(client);
      closeQuietly(upstream);
    }
  }
}
