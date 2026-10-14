package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.System.Logger.Level;
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
 * <p>Inside the load one saga, {@code <seed>-0} (never given a {@code Completed} event), is held at
 * the window on purpose: on {@code node-a} its first {@code Ship} waits before the bus until
 * another node's timeout runner has claimed the saga and stored its first compensation command. The
 * other nodes deliver none of its events until the hold ends (their timeout runners claim without a
 * delivery); {@code node-a}'s own timeout runner cannot claim it (the step holds that node's step
 * lock); the seeded retry-later failures skip the saga's commands. So that one forward command
 * reaches its commit after the compensation, however fast the machine runs the rest of the load.
 *
 * <p>With the fence on, after the load has quiesced: the invariant query over {@code command_inbox}
 * returns no row, every saga row is terminal, each compensation is stored once, and in {@code
 * event_stream} no saga has a {@code Reserved} or {@code Shipped} after its first {@code Released}
 * or {@code Refunded}. With the fence off, the same load stores the held saga's {@code Ship} after
 * its compensation, and the invariant query returns it: the proof that the test can fail does not
 * rest on how often the seeded timing alone reaches the window.
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

  private static final System.Logger LOG =
      System.getLogger(SagaForwardFenceStressIT.class.getName());

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

    assertThat(load.fencedByNode().get(Held.HOLDER))
        .as("the held Ship reaches its commit after the compensation: the fence refuses it")
        .isPositive();

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
    long seed = 1;
    run(seed, false);

    List<LateForwardCommand> late = SagaForwardFenceInvariant.lateForwardCommands(dataSource);
    LOG.log(Level.INFO, "fence off: forward commands stored after a compensation: {0}", late);
    assertThat(SagaForwardFenceInvariant.forwardKeysOf(late))
        .as("the held saga's Ship is stored after its compensation")
        .anyMatch(key -> key.startsWith("saga:" + sagaOf(heldOrder(seed)).value() + ":"));
  }

  // ---------------------------------------------------------------------------------------------

  /** Runs the load of {@code seed} to quiescence; the sagas of seed {@code s} are {@code s-n}. */
  private Load run(long seed, boolean fence) throws Exception {
    Random random = new Random(seed);
    List<Node> nodes = new ArrayList<>();
    Set<String> failedOnce = ConcurrentHashMap.newKeySet();
    Held held = new Held(heldOrder(seed));
    for (String name : NODES) {
      Node node = pooledNode(name, fence, Clock.systemUTC(), DEADLINE);
      node.bus.beforeEveryCommand =
          command -> {
            held.beforeCommand(name, command);
            pauseBeforeAForwardCommand(command);
          };
      node.bus.failure =
          (command, key) ->
              isForward(command)
                      && !held.covers(command)
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
      boolean completes = random.nextBoolean();
      if (completes && !order.equals(heldOrder(seed))) {
        // the held saga ends by its deadline: a completion would leave nothing to claim
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
    AtomicInteger redeliveries = new AtomicInteger();
    AtomicInteger abandoned = new AtomicInteger();
    Map<String, Integer> redeliveredOn = new ConcurrentHashMap<>();
    Map<String, Integer> rowsByStatus;
    try {
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

      List<CompletableFuture<Void>> deliverers = new ArrayList<>();
      for (Node node : nodes) {
        for (int t = 0; t < DELIVERING_THREADS_PER_NODE; t++) {
          List<EventEnvelope> plan = interleave(eventsBySaga, random);
          long pauseSeed = random.nextLong();
          deliverers.add(
              onAnotherThread(
                  () -> {
                    PAUSES.set(new Random(pauseSeed));
                    for (EventEnvelope event : plan) {
                      held.awaitTheHolderBefore(node.name, event);
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
      rowsByStatus = awaitEveryRowTerminal(seed);
    } finally {
      // Also when the load fails: the pollers stop before their nodes' pools close.
      stop.set(true);
      for (CompletableFuture<Void> poller : pollers) {
        awaitQuietly(poller);
      }
    }
    for (CompletableFuture<Void> poller : pollers) {
      assertThat(outcomeOf(poller, PATIENCE_SECONDS)).as("a timeout poller").isNull();
    }
    assertThat(held.outcome)
        .as("the held Ship waited for the other node's compensation")
        .isEqualTo(Held.RELEASED_AFTER_THE_COMPENSATION);

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
    LOG.log(Level.INFO, "saga forward fence load: {0}", load);
    // The next seed's nodes open pools of their own; these give their connections back.
    nodes.forEach(this::close);
    return load;
  }

  /** The order whose first {@code Ship} is held at the window in the load of {@code seed}. */
  private static String heldOrder(long seed) {
    return seed + "-0";
  }

  /**
   * The saga held at the window. On {@link #HOLDER}, the saga's first {@code Ship} waits before the
   * bus until the saga's first compensation command is stored. The other nodes deliver none of the
   * saga's events until that hold ends — so the holder creates the saga's row and reads it in its
   * forward phase, no other node stores the held command's key first, and no other node keeps its
   * step lock on the saga: their timeout runners claim it at its deadline.
   */
  private final class Held {
    static final String HOLDER = "node-a";
    static final String RELEASED_AFTER_THE_COMPENSATION = "released after the compensation";

    private final String order;
    private final AtomicBoolean taken = new AtomicBoolean();
    private volatile boolean released;
    volatile String outcome = "never reached";

    Held(String order) {
      this.order = order;
    }

    /** Whether {@code command} is a forward command of the held saga. */
    boolean covers(Command command) {
      return isForward(command) && order.equals(orderOf(command));
    }

    /**
     * On a delivering thread: a node other than the holder delivers the saga's events only after
     * the hold ends; it fails the delivering thread if the hold has not ended within the patience.
     */
    void awaitTheHolderBefore(String node, EventEnvelope event) {
      if (HOLDER.equals(node) || !order.equals(orderOf(event)) || released) {
        return;
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
      while (!released) {
        if (System.nanoTime() > deadline) {
          throw new AssertionError(
              node
                  + " waited "
                  + PATIENCE_SECONDS
                  + " s for the hold of saga "
                  + order
                  + " on "
                  + HOLDER
                  + " to end; the hold: "
                  + outcome);
        }
        sleepMillis(5);
      }
    }

    /** On the holder's dispatching thread, before the bus: holds the saga's first {@code Ship}. */
    void beforeCommand(String node, Command command) {
      if (HOLDER.equals(node)
          && covers(command)
          && command instanceof Ship
          && taken.compareAndSet(false, true)) {
        try {
          outcome = awaitTheFirstCompensation();
        } finally {
          released = true;
        }
      }
    }

    private String awaitTheFirstCompensation() {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT count(*) FROM command_inbox WHERE idempotency_key LIKE ?")) {
        ps.setString(1, "saga:" + sagaOf(order).value() + ":episode:%:comp:0");
        while (System.nanoTime() < deadline) {
          try (ResultSet rs = ps.executeQuery()) {
            rs.next();
            if (rs.getLong(1) > 0) {
              return RELEASED_AFTER_THE_COMPENSATION;
            }
          }
          sleepMillis(10);
        }
        return "released without a compensation after " + PATIENCE_SECONDS + " s";
      } catch (SQLException failed) {
        return "released on " + causeChain(failed);
      }
    }
  }

  private static String orderOf(EventEnvelope event) {
    return switch (event.event()) {
      case Opened opened -> opened.orderId();
      case Advanced advanced -> advanced.orderId();
      case Completed completed -> completed.orderId();
      default -> null;
    };
  }

  private static String orderOf(Command command) {
    return switch (command) {
      case Reserve reserve -> reserve.orderId();
      case Ship ship -> ship.orderId();
      default -> null;
    };
  }

  private static void sleepMillis(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while the load waits", interrupted);
    }
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
        backOff(delivery);
      }
    }
    return false;
  }

  /** A subscription's pause before it delivers a failed event again: 1, 2, 4 … up to 32 ms. */
  private static void backOff(int delivery) {
    try {
      TimeUnit.MILLISECONDS.sleep(Math.min(32, 1L << Math.min(delivery - 1, 5)));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted before a redelivery", interrupted);
    }
  }

  /** Waits a bounded time for a thread of the load to end, without failing the test here. */
  private static void awaitQuietly(CompletableFuture<Void> thread) {
    try {
      thread.get(PATIENCE_SECONDS, TimeUnit.SECONDS);
    } catch (Exception ended) {
      // its outcome is asserted after the load when the load got that far
    }
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
