package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.postgres.SagaFenceNodes.onAnotherThread;
import static org.streamrune.postgres.SagaFenceNodes.outcomeOf;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.postgres.SagaFenceNodes.Node;
import org.streamrune.postgres.SagaFenceNodes.Opened;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A process cut off from the database while it is inside a saga's forward append delays another
 * process's timeout claim of that saga by at most the append's statement timeout.
 *
 * <p>Node A reaches the database through a peer that goes silent without closing its sockets: the
 * database host vanishing, from A's side. A's forward append is stopped right after its fence read
 * — its backend idle in transaction, holding the saga's shared fence lock and the command's inbox
 * key — and the peer goes silent. Node B, on a direct connection, claims the saga at its deadline:
 * its exclusive fence lock waits until the append transaction's idle bound (A's statement timeout,
 * two seconds) ends A's session, and no longer. When A's socket times out its delivery fails, and
 * its redelivery, over a new connection, resumes the compensation episode B opened.
 */
@Testcontainers
@Timeout(120)
class SagaForwardFencePartitionIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_fence_partition");

  private static final String ORDER = "1";
  private static final Duration STATEMENT_TIMEOUT = Duration.ofSeconds(2);
  private static final long SLACK_MILLIS = 1_500;

  static PGSimpleDataSource direct;

  private SagaFenceNodes nodes;
  private SilentPeerProxy peer;

  @BeforeAll
  static void schema() {
    direct = dataSource(PG.getJdbcUrl(), "partition-test");
    new PostgresEventStoreFactory(direct, SagaFenceNodes.REGISTRY).create();
  }

  @BeforeEach
  void fresh() throws Exception {
    nodes = new SagaFenceNodes(direct);
    nodes.clean();
    peer = new SilentPeerProxy(PG.getHost(), PG.getMappedPort(5432));
  }

  @AfterEach
  void closeAll() {
    nodes.closeAll();
    peer.close();
  }

  @Test
  void aNodeCutOffInsideItsAppend_delaysTheOtherNodesClaimByAtMostTheStatementTimeout()
      throws Exception {
    PGSimpleDataSource throughThePeer = dataSource(peer.jdbcUrl(PG.getDatabaseName()), "node-a");
    // A read on a silent socket ends after ten seconds, so A's thread comes back.
    throughThePeer.setSocketTimeout(10);
    PausingDataSource pausing = new PausingDataSource(throughThePeer);
    Node a = nodes.node("node-a", pausing, STATEMENT_TIMEOUT, PostgresSagaStore.ROW_WRITE_TIMEOUT);
    Node b = nodes.node("node-b", dataSource(PG.getJdbcUrl(), "node-b"));
    // B's compensation stops after its first command, so A finds the episode still open.
    b.refund.failure = () -> new IllegalStateException("connection reset");
    EventEnvelope opened = nodes.append(new Opened(ORDER));

    PausingDataSource.Pause pause = pausing.pauseBefore("UPDATE global_offset_sequence");
    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(opened));
    pause.awaitPaused();
    long idleSince = System.nanoTime();
    int appendPid = nodes.idleInTransaction("node-a");
    peer.silenceOpenConnections();
    pause.release();

    nodes.timeOut(b);
    long claimedAfter = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - idleSince);

    System.out.printf(
        "B's claim returned %d ms after A's append went idle (statement timeout %d ms)%n",
        claimedAfter, STATEMENT_TIMEOUT.toMillis());
    assertThat(nodes.row(ORDER).orElseThrow().episodeVersion()).as("B claimed").isNotNull();
    assertThat(claimedAfter)
        .as("B's claim waits for A's silent append no longer than A's statement timeout")
        .isLessThan(STATEMENT_TIMEOUT.toMillis() + SLACK_MILLIS);
    nodes.awaitGone(appendPid);
    assertThat(nodes.backendsNamed("node-a")).as("A's session is gone").isZero();
    assertThat(nodes.storedOrder(ORDER)).containsExactly("Released");
    nodes.assertTheFenceHeld(ORDER);

    // A's socket times out: the append fails, and the delivery with it, to be redelivered.
    assertThat(outcomeOf(delivery)).isInstanceOf(EventStoreException.class);
    assertThat(nodes.storedOrder(ORDER)).as("nothing of A's command").containsExactly("Released");

    // The peer forwards new connections again: A's redelivery meets the open episode and resumes
    // it.
    a.deliver(opened);
    assertThat(nodes.storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(nodes.status(ORDER)).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(nodes.inboxKeys(ORDER))
        .as("each compensation command once, and no forward command")
        .hasSize(2)
        .allMatch(key -> key.contains(":episode:"));
    nodes.assertTheFenceHeld(ORDER);
  }

  private static PGSimpleDataSource dataSource(String url, String applicationName) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setUrl(url);
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    ds.setApplicationName(applicationName);
    return ds;
  }
}
