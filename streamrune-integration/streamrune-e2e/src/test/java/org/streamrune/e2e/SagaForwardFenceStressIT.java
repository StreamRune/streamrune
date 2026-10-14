package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.e2e.SagaForwardFenceInvariant.LateForwardCommand;

/**
 * Three processes on one PostgreSQL database under a seeded load: thirty order sagas (a start
 * event, eight {@code Advanced} events, and for about half of them a {@code Completed} event),
 * every trigger event delivered on every node by two delivering threads each, a timeout runner on
 * every node polling without pause against a 300 ms deadline on the wall clock. Each delivering
 * thread delivers every saga's events in the saga's order, interleaved across ten sagas at a time
 * in an order drawn from the seed; before each forward command it pauses for 0–20 ms (drawn from
 * the seed); one forward command in eight (chosen from the seed and the command's key) fails once
 * with a retry-later failure before it reaches the bus, and a delivery that fails is delivered
 * again, as a subscription does. Each node holds a connection pool of its own.
 *
 * <p>With the fence on, after the load has quiesced: the invariant query over {@code command_inbox}
 * returns no row, every saga row is terminal, each compensation is stored once, and in {@code
 * event_stream} no saga has a {@code Reserved} or {@code Shipped} after its first {@code Released}
 * or {@code Refunded}. With the fence off, the same load breaks the invariant for at least one saga
 * over the five seeds, which proves the load reaches the window the fence closes.
 */
@Timeout(600)
class SagaForwardFenceStressIT extends SagaForwardFenceNodes {

  private static final int SAGAS = 30;
  private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
  private static final int DELIVERING_THREADS_PER_NODE = 2;
  private static final Duration DEADLINE = Duration.ofMillis(300);
  private static final int MAX_PAUSE_MILLIS = 20;
  private static final int ADVANCED_EVENTS_PER_SAGA = 8;

  /**
   * A delivering thread takes its next event from one of the first this-many sagas it has not
   * finished: a saga's events reach the nodes over a span around its deadline, as they do when the
   * sagas of a busy application overlap, instead of over the whole run.
   */
  private static final int SAGAS_IN_FLIGHT = 10;

  private static final int ONE_FORWARD_COMMAND_IN = 8;
  private static final int MAX_DELIVERIES_OF_AN_EVENT = 50;

  /** The pause source of the delivering thread; unset on every other thread. */
  private static final ThreadLocal<Random> PAUSES = new ThreadLocal<>();

  /** What one run of the load did, for the report line. */
  record Load(
      long seed,
      boolean fence,
      Map<String, Integer> fencedByNode,
      int redeliveries,
      Map<String, Integer> redeliveredOn,
      int abandoned,
      int timeoutPollFailures,
      Map<String, Integer> rowsByStatus,
      Map<String, String> firstFailures) {}

  @ParameterizedTest(name = "seed {0}")
  @ValueSource(longs = {1, 2, 3, 4, 5})
  void noForwardCommandIsStoredAfterItsSagasFirstCompensation_fenceOn(long seed) throws Exception {
    Load load = run(seed, true);

    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
    assertThat(load.rowsByStatus().keySet())
        .as("every row is terminal")
        .isSubsetOf("COMPLETED", "COMPENSATED", "FAILED");
    assertThat(load.rowsByStatus().values().stream().mapToInt(Integer::intValue).sum())
        .isEqualTo(SAGAS);
    assertThat(SagaForwardFenceInvariant.sagasWithACompensationStoredTwice(probe, dataSource))
        .isEmpty();
    assertThat(SagaForwardFenceInvariant.sagasWithAForwardEventAfterACompensationEvent(probe))
        .isEmpty();
    assertThat(load.abandoned()).as("every event was delivered in the end").isZero();
  }

  @Test
  void theSameLoadWithTheFenceOff_breaksTheInvariant() throws Exception {
    List<LateForwardCommand> late = new ArrayList<>();
    for (long seed = 1; seed <= 5 && late.isEmpty(); seed++) {
      run(seed, false);
      late.addAll(SagaForwardFenceInvariant.lateForwardCommands(dataSource));
    }
    System.out.println("fence off: forward commands stored after a compensation: " + late);
    assertThat(late).isNotEmpty();
  }

  // ---------------------------------------------------------------------------------------------

  /** Runs the load of {@code seed} to quiescence; the sagas of seed {@code s} are {@code s-n}. */
  private Load run(long seed, boolean fence) throws Exception {
    Random random = new Random(seed);
    List<Node> nodes = new ArrayList<>();
    Set<String> failedOnce = ConcurrentHashMap.newKeySet();
    for (String name : NODES) {
      Node node = pooledNode(name, fence, Clock.systemUTC(), DEADLINE);
      node.bus.beforeEveryCommand = SagaForwardFenceStressIT::pauseBeforeAForwardCommand;
      node.bus.failure =
          (command, key) ->
              isForward(command)
                      && Math.floorMod(
                              (seed + ":" + key.value()).hashCode(), ONE_FORWARD_COMMAND_IN)
                          == 0
                      && failedOnce.add(key.value())
                  ? new CircuitBreakerOpenException("the downstream service is unavailable")
                  : null;
      nodes.add(node);
    }

    Map<String, List<EventEnvelope>> eventsBySaga = new LinkedHashMap<>();
    for (int n = 0; n < SAGAS; n++) {
      String order = seed + "-" + n;
      List<DomainEvent> triggers = new ArrayList<>();
      triggers.add(new Opened(order));
      for (int advanced = 0; advanced < ADVANCED_EVENTS_PER_SAGA; advanced++) {
        triggers.add(new Advanced(order));
      }
      if (random.nextBoolean()) {
        triggers.add(new Completed(order));
      }
      List<EventEnvelope> appended = new ArrayList<>();
      for (DomainEvent trigger : triggers) {
        appended.add(append(trigger));
      }
      eventsBySaga.put(order, appended);
    }

    Map<String, String> firstFailures = new ConcurrentHashMap<>();
    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger pollFailures = new AtomicInteger();
    List<CompletableFuture<Void>> pollers = new ArrayList<>();
    for (Node node : nodes) {
      pollers.add(
          onAnotherThread(
              () -> {
                while (!stop.get()) {
                  try {
                    node.pollTimeouts();
                  } catch (RuntimeException failed) {
                    pollFailures.incrementAndGet();
                    firstFailures.putIfAbsent("timeout poll", causeChain(failed));
                  }
                }
              }));
    }

    AtomicInteger redeliveries = new AtomicInteger();
    AtomicInteger abandoned = new AtomicInteger();
    Map<String, Integer> redeliveredOn = new ConcurrentHashMap<>();
    List<CompletableFuture<Void>> deliverers = new ArrayList<>();
    for (int n = 0; n < nodes.size(); n++) {
      Node node = nodes.get(n);
      for (int t = 0; t < DELIVERING_THREADS_PER_NODE; t++) {
        List<EventEnvelope> plan = interleave(eventsBySaga, random);
        long pauseSeed = random.nextLong();
        deliverers.add(
            onAnotherThread(
                () -> {
                  PAUSES.set(new Random(pauseSeed));
                  for (EventEnvelope event : plan) {
                    if (!deliverUntilAccepted(
                        node, event, redeliveries, redeliveredOn, firstFailures)) {
                      abandoned.incrementAndGet();
                    }
                  }
                }));
      }
    }
    for (CompletableFuture<Void> deliverer : deliverers) {
      assertThat(outcomeOf(deliverer, 300)).as("a delivering thread").isNull();
    }

    Map<String, Integer> rowsByStatus = awaitEveryRowTerminal(seed);
    stop.set(true);
    for (CompletableFuture<Void> poller : pollers) {
      assertThat(outcomeOf(poller, PATIENCE_SECONDS)).as("a timeout poller").isNull();
    }

    Map<String, Integer> fencedByNode = new LinkedHashMap<>();
    nodes.forEach(node -> fencedByNode.put(node.name, node.fencedSamples.size()));
    Load load =
        new Load(
            seed,
            fence,
            fencedByNode,
            redeliveries.get(),
            new TreeMap<>(redeliveredOn),
            abandoned.get(),
            pollFailures.get(),
            rowsByStatus,
            new TreeMap<>(firstFailures));
    System.out.println("saga forward fence load: " + load);
    // The next seed's nodes open pools of their own; these give their connections back.
    nodes.forEach(this::close);
    return load;
  }

  /**
   * Each saga's events in the saga's order, the sagas interleaved in an order drawn from {@code
   * random}.
   */
  private static List<EventEnvelope> interleave(
      Map<String, List<EventEnvelope>> eventsBySaga, Random random) {
    List<ArrayDeque<EventEnvelope>> queues = new ArrayList<>();
    eventsBySaga.values().forEach(events -> queues.add(new ArrayDeque<>(events)));
    List<EventEnvelope> plan = new ArrayList<>();
    while (!queues.isEmpty()) {
      int pick = random.nextInt(Math.min(SAGAS_IN_FLIGHT, queues.size()));
      ArrayDeque<EventEnvelope> queue = queues.get(pick);
      plan.add(queue.poll());
      if (queue.isEmpty()) {
        queues.remove(pick);
      }
    }
    return plan;
  }

  /** Delivers until the listener accepts the event, as a subscription redelivers a failed one. */
  private static boolean deliverUntilAccepted(
      Node node,
      EventEnvelope event,
      AtomicInteger redeliveries,
      Map<String, Integer> redeliveredOn,
      Map<String, String> firstFailures) {
    for (int delivery = 1; delivery <= MAX_DELIVERIES_OF_AN_EVENT; delivery++) {
      try {
        node.deliver(event);
        return true;
      } catch (RuntimeException failed) {
        redeliveries.incrementAndGet();
        redeliveredOn.merge(failed.getClass().getSimpleName(), 1, Integer::sum);
        firstFailures.putIfAbsent(failed.getClass().getSimpleName(), causeChain(failed));
      }
    }
    return false;
  }

  /** The failure and its causes, for the report line. */
  private static String causeChain(Throwable failure) {
    StringBuilder chain = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      chain.append(chain.isEmpty() ? "" : " <- ").append(t);
    }
    return chain.toString();
  }

  private static boolean isForward(Command command) {
    return command instanceof Reserve || command instanceof Ship;
  }

  private static void pauseBeforeAForwardCommand(Command command) {
    Random pauses = PAUSES.get();
    if (pauses == null || !isForward(command)) {
      return;
    }
    try {
      TimeUnit.MILLISECONDS.sleep(pauses.nextInt(MAX_PAUSE_MILLIS + 1));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted in the pause before a forward command");
    }
  }

  /** Waits while the timeout runners poll until every saga row of the seed is terminal. */
  private Map<String, Integer> awaitEveryRowTerminal(long seed) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    while (true) {
      Map<String, Integer> byStatus = rowsByStatus(seed);
      boolean allTerminal =
          byStatus.keySet().stream()
              .allMatch(status -> Set.of("COMPLETED", "COMPENSATED", "FAILED").contains(status));
      if (allTerminal || System.nanoTime() > deadline) {
        return byStatus;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
  }

  private Map<String, Integer> rowsByStatus(long seed) throws SQLException {
    Map<String, Integer> byStatus = new TreeMap<>();
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT status, count(*) FROM saga_state WHERE saga_id LIKE ? GROUP BY status")) {
      ps.setString(1, "order-saga-" + seed + "-%");
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          byStatus.put(rs.getString(1), rs.getInt(2));
        }
      }
    }
    return byStatus;
  }

  private static Throwable outcomeOf(CompletableFuture<Void> thread, long seconds)
      throws InterruptedException {
    try {
      thread.get(seconds, TimeUnit.SECONDS);
      return null;
    } catch (java.util.concurrent.ExecutionException failed) {
      return failed.getCause();
    } catch (java.util.concurrent.TimeoutException stuck) {
      throw new AssertionError("a thread of the load did not finish", stuck);
    }
  }
}
