package org.streamrune.crypto.sqlite;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * The {@link CryptoForgetSignal} of a SQLite database: a forget published here reaches, in this
 * JVM, every subscriber of every open signal on the same database file, the publisher's own
 * included. One SQLite database serves exactly one application instance (its instance guard refuses
 * a second process), so the JVM is the whole fleet and no transport is needed: this replaces
 * PostgreSQL's {@code LISTEN/NOTIFY}.
 *
 * <p><b>Delivery.</b> {@link #publish(String)} hands the token to each subscriber on the
 * publisher's thread, before it returns. A subscriber that throws a {@link RuntimeException} is
 * logged and stops neither the other subscribers nor the publisher: a publish never fails the
 * {@code deleteKey} it follows. A {@code null} token is ignored.
 *
 * <p><b>Hearing.</b> A subscriber hears from the moment {@link #subscribe(Consumer, Runnable)}
 * registers it: its {@code onListening} runs once, there, on the subscribing thread, and a token
 * published meanwhile waits until it has returned, so no forget falls between the eviction {@code
 * onListening} does and the first token. There is no connection to lose, so it never runs again.
 * {@link #isSubscribed()} and {@link #isListening()} are {@code true} from the first subscribe
 * until {@link #close()}.
 *
 * <p><b>Closing.</b> {@link #close()} removes this signal's subscribers: nothing reaches them
 * afterwards, while the other signals on the file keep hearing. A subscribe after the close
 * registers nothing; a publish after it still reaches the other signals. Callbacks run under a lock
 * of their subscriber, so they must not block on another subscriber's work.
 */
public final class InProcessCryptoForgetSignal implements CryptoForgetSignal {

  private static final Logger log = LoggerFactory.getLogger(InProcessCryptoForgetSignal.class);

  /** The subscribers of every open signal in this JVM, by canonical database file. */
  private static final ConcurrentHashMap<Path, Set<Subscriber>> CHANNELS =
      new ConcurrentHashMap<>();

  private final Path file;

  /** This signal's subscribers; guarded by {@code lifecycle}. */
  private final List<Subscriber> own = new ArrayList<>();

  private final Object lifecycle = new Object();
  private boolean subscribed;
  private boolean closed;

  /**
   * A signal on the database file of {@code database}. The signal keeps the file's path, not the
   * handle: closing the handle stays the caller's.
   *
   * @param database the database whose forgets the signal carries
   * @throws IllegalArgumentException if {@code database} is {@code null}
   */
  public InProcessCryptoForgetSignal(SqliteDatabase database) {
    if (database == null) {
      throw new IllegalArgumentException("database must not be null");
    }
    this.file = database.file();
  }

  /**
   * One registered callback pair. Its monitor orders delivery after its {@code onListening} and
   * stops delivery once it is removed.
   */
  private static final class Subscriber {
    private final Consumer<String> onForget;
    private final Runnable onListening;
    private boolean removed;

    private Subscriber(Consumer<String> onForget, Runnable onListening) {
      this.onForget = onForget;
      this.onListening = onListening;
    }

    private synchronized void startHearing() {
      onListening.run();
    }

    private synchronized void deliver(String token) {
      if (removed) {
        return;
      }
      try {
        onForget.accept(token);
      } catch (RuntimeException e) {
        log.warn("A crypto-forget listener threw while handling a token", e);
      }
    }

    private synchronized void remove() {
      removed = true;
    }
  }

  @Override
  public void publish(String token) {
    if (token == null) {
      return;
    }
    Set<Subscriber> channel = CHANNELS.get(file);
    if (channel == null) {
      return;
    }
    for (Subscriber subscriber : channel) {
      subscriber.deliver(token);
    }
  }

  @Override
  public void subscribe(Consumer<String> onForget, Runnable onListening) {
    if (onForget == null) {
      throw new IllegalArgumentException("listener must not be null");
    }
    if (onListening == null) {
      throw new IllegalArgumentException("onListening must not be null");
    }
    Subscriber subscriber = new Subscriber(onForget, onListening);
    // The subscriber's monitor is held from before it joins the channel until its onListening has
    // returned, so a token published meanwhile waits for the eviction instead of preceding it.
    synchronized (subscriber) {
      synchronized (lifecycle) {
        if (closed) {
          return;
        }
        own.add(subscriber);
        // compute, not computeIfAbsent(...).add: a close of another signal that empties the
        // channel removes it atomically, and an add to the removed set would never be heard.
        CHANNELS.compute(
            file,
            (f, channel) -> {
              Set<Subscriber> joined = channel == null ? ConcurrentHashMap.newKeySet() : channel;
              joined.add(subscriber);
              return joined;
            });
        subscribed = true;
      }
      subscriber.startHearing();
    }
  }

  @Override
  public boolean isSubscribed() {
    synchronized (lifecycle) {
      return subscribed && !closed;
    }
  }

  @Override
  public boolean isListening() {
    return isSubscribed();
  }

  /** Removes this signal's subscribers; calling it again does nothing. */
  @Override
  public void close() {
    synchronized (lifecycle) {
      if (closed) {
        return;
      }
      closed = true;
      for (Subscriber subscriber : own) {
        subscriber.remove();
        CHANNELS.computeIfPresent(
            file,
            (f, channel) -> {
              channel.remove(subscriber);
              return channel.isEmpty() ? null : channel;
            });
      }
      own.clear();
    }
  }

  @Override
  public String toString() {
    return "InProcessCryptoForgetSignal[" + file + "]";
  }
}
