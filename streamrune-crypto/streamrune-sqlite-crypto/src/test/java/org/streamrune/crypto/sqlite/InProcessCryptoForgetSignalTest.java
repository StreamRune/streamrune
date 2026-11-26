package org.streamrune.crypto.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * The in-process forget signal delivers a published token to every subscriber of every open signal
 * on the same database file in this JVM, after each subscriber's {@code onListening}, and {@code
 * close()} stops delivery to the closed signal's subscribers.
 */
class InProcessCryptoForgetSignalTest {

  @TempDir Path dir;

  private SqliteDatabase database;
  private SqliteDatabase otherDatabase;

  @BeforeEach
  void open() {
    database = SqliteDatabase.open(SqliteFiles.dataSource(dir.resolve("signal.db")));
    otherDatabase = SqliteDatabase.open(SqliteFiles.dataSource(dir.resolve("other.db")));
  }

  @AfterEach
  void close() {
    database.close();
    otherDatabase.close();
  }

  @Test
  void aTokenReachesEverySubscriberOfEverySignalOnTheFile_andNoOtherFile() {
    List<String> own = new CopyOnWriteArrayList<>();
    List<String> peer = new CopyOnWriteArrayList<>();
    List<String> elsewhere = new CopyOnWriteArrayList<>();
    try (var signal = new InProcessCryptoForgetSignal(database);
        var sameFile = new InProcessCryptoForgetSignal(database);
        var otherFile = new InProcessCryptoForgetSignal(otherDatabase)) {
      signal.subscribe(own::add);
      sameFile.subscribe(peer::add);
      otherFile.subscribe(elsewhere::add);

      signal.publish("token-1");
      sameFile.publish("token-2");

      assertThat(own).containsExactly("token-1", "token-2");
      assertThat(peer).containsExactly("token-1", "token-2");
      assertThat(elsewhere).isEmpty();
    }
  }

  @Test
  void onListeningRunsOnceAtSubscribe_andTheSignalHearsUntilClosed() {
    var signal = new InProcessCryptoForgetSignal(database);
    List<String> events = new CopyOnWriteArrayList<>();
    assertThat(signal.isSubscribed()).isFalse();
    assertThat(signal.isListening()).isFalse();

    signal.subscribe(events::add, () -> events.add("listening"));
    signal.publish("token");

    assertThat(events).containsExactly("listening", "token");
    assertThat(signal.isSubscribed()).isTrue();
    assertThat(signal.isListening()).isTrue();
    signal.close();
    assertThat(signal.isSubscribed()).isFalse();
    assertThat(signal.isListening()).isFalse();
  }

  @Test
  void closeStopsDeliveryToItsSubscribers_theOtherSignalsKeepHearing() {
    List<String> closedOnes = new CopyOnWriteArrayList<>();
    List<String> openOnes = new CopyOnWriteArrayList<>();
    var closing = new InProcessCryptoForgetSignal(database);
    try (var staying = new InProcessCryptoForgetSignal(database)) {
      closing.subscribe(closedOnes::add);
      staying.subscribe(openOnes::add);

      closing.close();
      closing.close();
      closing.publish("after-close");
      staying.publish("still-open");

      assertThat(closedOnes).isEmpty();
      assertThat(openOnes).containsExactly("after-close", "still-open");
      closing.subscribe(closedOnes::add, () -> closedOnes.add("listening"));
      staying.publish("again");
      assertThat(closedOnes).as("a subscribe after close registers nothing").isEmpty();
      assertThat(closing.isSubscribed()).isFalse();
    }
  }

  @Test
  void aThrowingSubscriberStopsNeitherTheOthersNorThePublisher() {
    List<String> heard = new CopyOnWriteArrayList<>();
    try (var signal = new InProcessCryptoForgetSignal(database)) {
      signal.subscribe(
          token -> {
            throw new IllegalStateException("listener fails");
          });
      signal.subscribe(heard::add);

      signal.publish("token");

      assertThat(heard).containsExactly("token");
    }
  }

  @Test
  void aNullTokenIsIgnored_aPublishWithoutSubscribersDoesNothing() {
    List<String> heard = new CopyOnWriteArrayList<>();
    try (var lonely = new InProcessCryptoForgetSignal(otherDatabase)) {
      lonely.publish("nobody hears this");
    }
    try (var signal = new InProcessCryptoForgetSignal(database)) {
      signal.subscribe(heard::add);
      signal.publish(null);
      assertThat(heard).isEmpty();
    }
  }

  /**
   * A token published while a subscriber's {@code onListening} runs waits until it has returned:
   * the eviction {@code CachedCryptoEngine} does there never follows a forget it should precede.
   */
  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aTokenPublishedDuringOnListeningIsDeliveredAfterIt() throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    var listeningStarted = new CountDownLatch(1);
    var releaseListening = new CountDownLatch(1);
    try (var signal = new InProcessCryptoForgetSignal(database);
        var publisher = new InProcessCryptoForgetSignal(database)) {
      Thread subscriber =
          Thread.ofPlatform()
              .start(
                  () ->
                      signal.subscribe(
                          events::add,
                          () -> {
                            listeningStarted.countDown();
                            await(releaseListening);
                            events.add("listening");
                          }));
      assertThat(listeningStarted.await(10, TimeUnit.SECONDS)).isTrue();
      Thread publishing = Thread.ofPlatform().start(() -> publisher.publish("token"));
      publishing.join(200);
      assertThat(events).as("the token waits for onListening").isEmpty();

      releaseListening.countDown();
      subscriber.join();
      publishing.join();

      assertThat(events).containsExactly("listening", "token");
    }
  }

  /**
   * Two cached engines on one file in this JVM, wired with signals on that file: a forget through
   * one evicts the other's cached plaintext at once.
   */
  @Test
  void aForgetThroughOneCachedEngineEvictsAnotherOnTheSameFile() {
    database.migrate(org.streamrune.sqlite.SqliteSchemaSeries.CRYPTO);
    SubjectId subject = SubjectId.of("signal-subject");
    try (var signalA = new InProcessCryptoForgetSignal(database);
        var signalB = new InProcessCryptoForgetSignal(database)) {
      CryptoEngine engineA = cached(signalA);
      CryptoEngine engineB = cached(signalB);
      byte[] plaintext = "pii".getBytes(StandardCharsets.UTF_8);
      byte[] ciphertext = engineA.encrypt(subject, plaintext);
      assertThat(engineB.decrypt(subject, ciphertext)).isEqualTo(plaintext);

      engineA.deleteKey(subject);

      org.assertj.core.api.Assertions.assertThatThrownBy(() -> engineB.decrypt(subject, ciphertext))
          .isInstanceOf(KeyNotFoundException.class);
    }
  }

  @Test
  void nullArgumentsAreRefused() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new InProcessCryptoForgetSignal(null))
        .withMessage("database must not be null");
    try (var signal = new InProcessCryptoForgetSignal(database)) {
      assertThatIllegalArgumentException()
          .isThrownBy(() -> signal.subscribe(null, () -> {}))
          .withMessage("listener must not be null");
      assertThatIllegalArgumentException()
          .isThrownBy(() -> signal.subscribe(token -> {}, null))
          .withMessage("onListening must not be null");
      assertThat(signal.toString()).contains(database.file().toString());
    }
  }

  private CryptoEngine cached(InProcessCryptoForgetSignal signal) {
    return CachedCryptoEngine.builder()
        .delegate(SqliteCryptoEngine.builder().database(database).build())
        .forgetSignal(signal)
        .build();
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
