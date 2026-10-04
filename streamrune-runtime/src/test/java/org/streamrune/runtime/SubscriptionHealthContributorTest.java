package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class SubscriptionHealthContributorTest {

  private EventStore eventStore;
  private OffsetStore offsetStore;
  private SubscriptionHealthContributor contributor;

  @BeforeEach
  void setUp() {
    eventStore = mock(EventStore.class);
    offsetStore = mock(OffsetStore.class);
    // A small lag threshold for tests: 100 events.
    contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
  }

  private SubscriptionLifecycle mockSubscription(SubscriptionLifecycleState state) {
    var sub = mock(SubscriptionLifecycle.class);
    when(sub.state()).thenReturn(state);
    return sub;
  }

  @Test
  void register_registersLagGaugeBeforePublishingTheEntry_OBS2() {
    // The live supplier lag gauge must be registered BEFORE the subscription entry is
    // published to the map. Otherwise a concurrent health scrape could observe the entry and call
    // recordSubscriptionLag first — registering a competing push gauge with the same name+tag — and
    // Micrometer would then dedup and DISCARD the supplier, permanently freezing
    // streamrune.subscriptions.lag for that subscription. We assert the order directly: at the
    // instant registerSubscriptionLagGauge fires, the subscription is not yet visible to health().
    stubGlobalHead(0);
    stubLastOffset("orders", 0);
    var healthSizeAtGaugeRegistration = new java.util.concurrent.atomic.AtomicInteger(-1);
    var holder = new SubscriptionHealthContributor[1];
    var orderProbingMetrics =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void registerSubscriptionLagGauge(
              org.streamrune.core.types.SubscriptionName subscriptionName,
              java.util.function.LongSupplier lagSupplier) {
            healthSizeAtGaugeRegistration.set(holder[0].health().size());
          }
        };
    var c = new SubscriptionHealthContributor(eventStore, offsetStore, 100, orderProbingMetrics);
    holder[0] = c;

    c.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    assertEquals(
        0,
        healthSizeAtGaugeRegistration.get(),
        "the lag gauge must be registered before the subscription entry is published");
    assertEquals(1, c.health().size(), "after register() the entry is published");
  }

  // ── replaceLifecycle preserves errorCount across a catch-up->live style swap ───────────

  @Test
  void replaceLifecycle_preservesErrorCountAcrossTheSwap() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.recordError("orders");
    contributor.recordError("orders");
    stubGlobalHead(10);
    stubLastOffset("orders", 10);
    assertEquals(2, errorCountOf("orders"), "two errors accrued on the standby view");

    var liveSubscription = mockSubscription(SubscriptionLifecycleState.RUNNING);
    contributor.replaceLifecycle("orders", liveSubscription);

    assertEquals(
        2,
        errorCountOf("orders"),
        "replaceLifecycle must preserve the accrued error count, unlike register()");
    assertEquals(SubscriptionHealth.Status.DEGRADED, contributor.overallStatus());
    verify(liveSubscription, atLeastOnce()).state();
  }

  @Test
  void replaceLifecycle_fallsBackToFreshRegister_whenNoExistingEntry() {
    stubGlobalHead(10);
    stubLastOffset("orders", 10);
    contributor.replaceLifecycle("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    assertEquals(1, contributor.health().size());
    assertEquals(
        0, errorCountOf("orders"), "no prior entry to preserve — falls back to register()");
  }

  @Test
  void replaceLifecycle_keepsATerminalEntryDown_insteadOfResurrectingIt() {
    // The old implementation fell back to register() for a terminal existing entry, which
    // creates a FRESH SubscriptionEntry with terminalError=false — DOWN silently became UP. This
    // pin used to assert exactly that (UP) under the comment "a terminal entry is not resurrected
    // by a lifecycle swap", which was the wrong claim for what the code actually did; it now
    // asserts the claim itself, correctly.
    stubGlobalHead(10);
    stubLastOffset("orders", 10);
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.markTerminalError("orders");
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());

    var freshLifecycleView = mockSubscription(SubscriptionLifecycleState.RUNNING);
    contributor.replaceLifecycle("orders", freshLifecycleView);

    assertEquals(
        SubscriptionHealth.Status.DOWN,
        contributor.overallStatus(),
        "a terminal entry must stay DOWN across a lifecycle swap — a swap is not a fresh"
            + " registration and must never silently resurrect a halted projection");
    // The lifecycle reference itself DOES swap (matches the non-terminal case) — only the terminal
    // verdict and accrued counters are preserved, not the old (likely-dead) lifecycle object.
    assertEquals(1, contributor.health().size());
  }

  // ── A persistently throwing recordSubscriptionLag sink must not flood the log ────────────

  @Test
  void computeHealth_throwingLagMetricSink_warnsOnceAcrossRepeatedScrapes() {
    // The guard around metrics.recordSubscriptionLag WARNed-with-stack-trace on EVERY
    // health() call — once per subscription, every scrape — so a persistently throwing sink
    // flooded the log at scrape cadence x N subscriptions. Two scrapes against one always-throwing
    // sink must log exactly one WARN, mirroring the class's own pushPathDeadWarned one-shot latch.
    stubGlobalHead(10);
    stubLastOffset("orders", 10);
    var throwingSink =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordSubscriptionLag(
              org.streamrune.core.types.SubscriptionName name, long lag) {
            throw new IllegalStateException("simulated metrics backend failure (lag gauge)");
          }
        };
    var throwingContributor =
        new SubscriptionHealthContributor(eventStore, offsetStore, 100, throwingSink);
    throwingContributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(SubscriptionHealthContributor.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      throwingContributor.health(); // scrape 1
      throwingContributor.health(); // scrape 2
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    long warnCount =
        appender.list.stream()
            .filter(
                e ->
                    e.getLevel() == ch.qos.logback.classic.Level.WARN
                        && e.getFormattedMessage()
                            .contains("Metrics recordSubscriptionLag failed for subscription"))
            .count();
    assertEquals(
        1, warnCount, "two scrapes against a persistently throwing sink must WARN exactly once");
  }

  private int errorCountOf(String name) {
    return contributor.health().stream()
        .filter(h -> h.name().equals(name))
        .findFirst()
        .map(SubscriptionHealth::errorCount)
        .orElse(-1);
  }

  private void stubGlobalHead(long head) {
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(head));
  }

  private void stubLastOffset(String name, long offset) {
    when(offsetStore.getLastOffset(ProjectionName.of(name))).thenReturn(GlobalOffset.of(offset));
  }

  @Test
  void healthReturnsUpWhenRunningLowLagNoErrors() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 45);

    var healths = contributor.health();

    assertEquals(1, healths.size());
    var h = healths.getFirst();
    assertEquals("orders", h.name());
    assertEquals(SubscriptionLifecycleState.RUNNING, h.state());
    assertEquals(5, h.lag());
    assertEquals(0, h.errorCount());
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }

  @Test
  void healthReturnsDownWhenStopped() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.STOPPED));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);

    var h = contributor.health().getFirst();
    assertEquals(SubscriptionHealth.Status.DOWN, h.status());
  }

  @Test
  void lagPastTheThreshold_readsDegraded_notDown() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 50);

    var h = contributor.health().getFirst();
    assertEquals(150, h.lag());
    assertEquals(
        SubscriptionHealth.Status.DEGRADED,
        h.status(),
        "a running subscription behind the head is still processing — DEGRADED, never DOWN");
  }

  @Test
  void lagAtTheThreshold_readsDegraded() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 100);

    var h = contributor.health().getFirst();
    assertEquals(100, h.lag());
    assertEquals(SubscriptionHealth.Status.DEGRADED, h.status());
  }

  @Test
  void lagJustBelowTheThreshold_readsUp() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 101);

    var h = contributor.health().getFirst();
    assertEquals(99, h.lag());
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }

  @Test
  void aProjectionCatchingUpFromOffsetZero_neverTurnsTheOverallStatusDown() {
    // Lag is measured against the shared checkpoint, so every replica reports the same value. A new
    // projection (or a reset one) replaying a large store reads the whole history as lag until the
    // single leader catches up — if that were DOWN, a readiness probe would take every replica out
    // of service for the length of the replay.
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.register("payments", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(5_000_000);
    stubLastOffset("orders", 0);
    stubLastOffset("payments", 5_000_000);

    assertEquals(SubscriptionHealth.Status.DEGRADED, contributor.overallStatus());
    var orders =
        contributor.health().stream().filter(h -> h.name().equals("orders")).findFirst().get();
    assertEquals(5_000_000, orders.lag(), "the lag itself stays visible in the health detail");
  }

  @Test
  void theDefaultLagThresholdIsOneThousandEvents() {
    assertEquals(1_000, new SubscriptionHealthContributor(eventStore, offsetStore).lagThreshold());
    assertEquals(
        1_000,
        new SubscriptionHealthContributor(
                eventStore, offsetStore, org.streamrune.core.StreamRuneMetrics.NOOP)
            .lagThreshold());
    assertEquals(100, contributor.lagThreshold());
  }

  @Test
  void aLagThresholdBelowOneEventIsRefused() {
    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SubscriptionHealthContributor(eventStore, offsetStore, 0));
    assertTrue(e.getMessage().contains("lagThreshold"), e.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> new SubscriptionHealthContributor(eventStore, offsetStore, -5));
  }

  @Test
  void onlyAStoppedOrTerminallyHaltedSubscriptionIsDown() {
    contributor.register("stopped", mockSubscription(SubscriptionLifecycleState.STOPPED));
    contributor.register("halted", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.markTerminalError("halted");
    contributor.register("lagging", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.register("failing", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.register("paused", mockSubscription(SubscriptionLifecycleState.PAUSED));
    for (int i = 0; i < 20; i++) {
      contributor.recordError("failing");
    }
    stubGlobalHead(10_000);
    stubLastOffset("stopped", 10_000);
    stubLastOffset("halted", 10_000);
    stubLastOffset("lagging", 0);
    stubLastOffset("failing", 10_000);
    stubLastOffset("paused", 10_000);

    var byName =
        contributor.health().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    SubscriptionHealth::name, SubscriptionHealth::status));
    assertEquals(SubscriptionHealth.Status.DOWN, byName.get("stopped"));
    assertEquals(SubscriptionHealth.Status.DOWN, byName.get("halted"));
    assertEquals(SubscriptionHealth.Status.DEGRADED, byName.get("lagging"));
    assertEquals(SubscriptionHealth.Status.DEGRADED, byName.get("failing"));
    assertEquals(SubscriptionHealth.Status.DEGRADED, byName.get("paused"));
  }

  @Test
  void theOverallStatusOfOneSampleIsTheWorstEntryInIt() {
    var up = health("a", SubscriptionHealth.Status.UP);
    var degraded = health("b", SubscriptionHealth.Status.DEGRADED);
    var down = health("c", SubscriptionHealth.Status.DOWN);

    assertEquals(
        SubscriptionHealth.Status.UP, SubscriptionHealthContributor.overallStatus(List.of()));
    assertEquals(
        SubscriptionHealth.Status.UP, SubscriptionHealthContributor.overallStatus(List.of(up)));
    assertEquals(
        SubscriptionHealth.Status.DEGRADED,
        SubscriptionHealthContributor.overallStatus(List.of(up, degraded)));
    assertEquals(
        SubscriptionHealth.Status.DOWN,
        SubscriptionHealthContributor.overallStatus(List.of(down, degraded, up)));
  }

  private static SubscriptionHealth health(String name, SubscriptionHealth.Status status) {
    return new SubscriptionHealth(name, SubscriptionLifecycleState.RUNNING, 0, 0, status);
  }

  @Test
  void healthReturnsDegradedWhenPaused() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.PAUSED));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);

    var h = contributor.health().getFirst();
    assertEquals(SubscriptionHealth.Status.DEGRADED, h.status());
  }

  @Test
  void healthReturnsDegradedWhenErrorsBelowThreshold() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    contributor.recordError("orders");
    contributor.recordError("orders");

    var h = contributor.health().getFirst();
    assertEquals(2, h.errorCount());
    assertEquals(SubscriptionHealth.Status.DEGRADED, h.status());
  }

  @Test
  void aLongRunOfConsecutiveErrors_readsDegraded_notDown() {
    // A subscription that keeps failing is still running and retrying (a dead-lettering or
    // skipping projection is even still advancing): DEGRADED. Only a stopped or terminally halted
    // subscription is DOWN.
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    for (int i = 0; i < 50; i++) {
      contributor.recordError("orders");
    }

    var h = contributor.health().getFirst();
    assertEquals(50, h.errorCount());
    assertEquals(SubscriptionHealth.Status.DEGRADED, h.status());
    assertEquals(SubscriptionHealth.Status.DEGRADED, contributor.overallStatus());
  }

  @Test
  void errorCountResetsOnSuccess() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    contributor.recordError("orders");
    contributor.recordError("orders");
    contributor.recordSuccess("orders");

    var h = contributor.health().getFirst();
    assertEquals(0, h.errorCount());
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }

  @Test
  void overallStatusDownIfAnySubscriptionDown() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.register("payments", mockSubscription(SubscriptionLifecycleState.STOPPED));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    stubLastOffset("payments", 50);

    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());
  }

  @Test
  void overallStatusDegradedIfAnyDegraded() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.register("payments", mockSubscription(SubscriptionLifecycleState.PAUSED));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    stubLastOffset("payments", 50);

    assertEquals(SubscriptionHealth.Status.DEGRADED, contributor.overallStatus());
  }

  @Test
  void overallStatusUpWhenNoSubscriptions() {
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus());
  }

  @Test
  void unregisterRemovesSubscription() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.unregister("orders");

    assertTrue(contributor.health().isEmpty());
  }

  @Test
  void markTerminalErrorForcesDownAndSurvivesRepeatedChecks() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    // Healthy before the halt: RUNNING, no lag, no errors.
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus());

    contributor.markTerminalError("orders");

    // The projection is definitively halted: DOWN regardless of its (still low) lag/error count,
    // and it STAYS registered so overallStatus keeps reporting DOWN across repeated checks.
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.health().getFirst().status());
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());
    assertFalse(contributor.health().isEmpty(), "a terminally halted subscription stays in view");
  }

  @Test
  void markTerminalErrorOnUnregisteredCreatesDownEntry() {
    // A projection that halted during catch-up, before any live subscription was registered.
    stubGlobalHead(50);
    stubLastOffset("orders", 10);

    contributor.markTerminalError("orders");

    var healths = contributor.health();
    assertEquals(1, healths.size());
    var h = healths.getFirst();
    assertEquals("orders", h.name());
    assertEquals(SubscriptionLifecycleState.STOPPED, h.state());
    assertEquals(SubscriptionHealth.Status.DOWN, h.status());
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());
  }

  @Test
  void reRegisterClearsTerminalError() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    contributor.markTerminalError("orders");
    stubGlobalHead(50);
    stubLastOffset("orders", 50);
    assertEquals(SubscriptionHealth.Status.DOWN, contributor.overallStatus());

    // A restart/reset re-registers the projection → a fresh, healthy entry clears the marking.
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus());
  }

  @Test
  void lagWithNoEventsInStore() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(0);
    stubLastOffset("orders", 0);

    var h = contributor.health().getFirst();
    assertEquals(0, h.lag());
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }

  @Test
  void lagWithNoOffsetYet() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    when(offsetStore.getLastOffset(ProjectionName.of("orders"))).thenReturn(GlobalOffset.initial());

    var h = contributor.health().getFirst();
    assertEquals(50, h.lag());
  }

  @Test
  void recordErrorOnUnknownSubscriptionIsNoOp() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(10);
    // Every name reads as caught up, so a phantom entry would show up in health() below instead of
    // failing inside its lag computation.
    when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.of(10));
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus(), "baseline");

    // As many errors as the threshold (3): a phantom entry created on demand would report DOWN.
    for (int i = 0; i < 3; i++) {
      contributor.recordError("nonexistent");
    }
    assertEquals(
        List.of("orders"),
        contributor.health().stream().map(SubscriptionHealth::name).toList(),
        "recordError on an unknown name must not create an entry for it");
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus());

    contributor.recordSuccess("nonexistent");
    assertEquals(
        List.of("orders"),
        contributor.health().stream().map(SubscriptionHealth::name).toList(),
        "recordSuccess on an unknown name must not create an entry for it");
    assertEquals(0, errorCountOf("orders"), "the registered subscription is untouched");
  }

  @Test
  void lagIsZeroWhenStoreDoesNotSupportLastGlobalOffset() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    when(eventStore.lastGlobalOffset())
        .thenThrow(new UnsupportedOperationException("not supported"));
    stubLastOffset("orders", 5);

    var h = contributor.health().getFirst();
    assertEquals(0, h.lag(), "unknown head must report lag 0 instead of failing");
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }

  @Test
  void recordsLagPerSubscriptionWhenMetricsSupplied() {
    var metrics = new RecordingStreamRuneMetrics();
    var withMetrics = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);
    withMetrics.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    withMetrics.register("payments", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 170);
    stubLastOffset("payments", 195);

    withMetrics.health();

    assertEquals(1, metrics.count("subscription.lag", "orders"));
    assertEquals(1, metrics.count("subscription.lag", "payments"));
    assertEquals(Long.valueOf(30), metrics.lag("orders"));
    assertEquals(Long.valueOf(5), metrics.lag("payments"));
  }

  @Test
  void health_survivesThrowingRecordSubscriptionLagMetric() {
    // computeHealth() called recordSubscriptionLag BARE, and computeHealth() runs once per
    // registered subscription on EVERY health() call — so a throwing metrics backend failed the
    // WHOLE /health scrape (every subscription, not just the one whose lag sample happened to
    // throw), even though the failure has nothing to do with any subscription's actual health.
    stubGlobalHead(10);
    stubLastOffset("orders", 5);
    var throwing =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordSubscriptionLag(
              org.streamrune.core.types.SubscriptionName subscriptionName, long lag) {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var c = new SubscriptionHealthContributor(eventStore, offsetStore, 100, throwing);
    c.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    var healths =
        assertDoesNotThrow(c::health, "a throwing metrics backend must not fail the whole scrape");

    assertEquals(1, healths.size());
    assertEquals(SubscriptionHealth.Status.UP, healths.get(0).status());
    assertEquals(5, healths.get(0).lag());
  }

  @Test
  void recordedLagReflectsLatestValueOnRepeatedHealthChecks() {
    var metrics = new RecordingStreamRuneMetrics();
    var withMetrics = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);
    withMetrics.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 170);
    withMetrics.health();
    assertEquals(Long.valueOf(30), metrics.lag("orders"));

    // Subscription caught up; the next health check must report the lower lag.
    stubLastOffset("orders", 198);
    withMetrics.health();
    assertEquals(Long.valueOf(2), metrics.lag("orders"));
  }

  @Test
  void liveLagGaugeReflectsCurrentLagWithoutAHealthScrape() {
    // The primary "falling behind" meter must be current on the metrics-scrape path, not
    // only after a health-endpoint scrape. register() wires a supplier gauge; sampling it (a
    // metrics
    // scrape) recomputes lag live — with no health() call at all.
    var metrics = new RecordingStreamRuneMetrics();
    var withMetrics = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);
    withMetrics.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 170);

    assertEquals(30, metrics.sampleLagGauge("orders"));

    // The subscription falls further behind; a metrics scrape sees the new lag immediately.
    stubLastOffset("orders", 100);
    assertEquals(100, metrics.sampleLagGauge("orders"));

    // No health-endpoint scrape ever ran, so the push path never fired.
    assertEquals(0, metrics.count("subscription.lag"), "the health-scrape push path must not fire");
    assertEquals(1, metrics.count("subscription.lagGauge.registered", "orders"));
  }

  @Test
  void currentLagIsZeroWhenTheStoreCannotReportItsHead() {
    // A store that structurally cannot report its head has no lag to report: 0 is the honest
    // answer, and this is a permanent capability gap, not a failure. (a transient READ
    // FAILURE is a different case — see lagGaugeHoldsItsLastKnownValueWhenTheStoreReadFails.)
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    when(eventStore.lastGlobalOffset())
        .thenThrow(new UnsupportedOperationException("store cannot report its head"));
    stubLastOffset("orders", 5);

    assertEquals(0, contributor.currentLag("orders"));
  }

  /**
   * {@code streamrune.subscriptions.lag} is the series production.md tells operators to alert on to
   * catch "a subscription that is retrying but not making progress". The supplier swallowed every
   * RuntimeException and returned 0 — the value of a perfectly caught-up subscription — so the
   * alert was structurally unfirable during exactly the store trouble it exists to catch. The gauge
   * must hold its last known value instead, matching what OutboxPoller/DeadLetterRetryRunner do
   * when their backlog sample fails.
   */
  @Test
  void lagGaugeHoldsItsLastKnownValueWhenTheStoreReadFails() {
    var metrics = new RecordingStreamRuneMetrics();
    var withMetrics = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);
    withMetrics.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 150);

    assertEquals(50, metrics.sampleLagGauge("orders"), "a healthy scrape establishes a known lag");

    // statement_timeout / revoked SELECT grant / momentarily exhausted pool.
    when(eventStore.lastGlobalOffset()).thenThrow(new IllegalStateException("statement timeout"));

    assertEquals(
        50,
        metrics.sampleLagGauge("orders"),
        "a failed store read must not report 0 — the healthiest possible value — while the"
            + " projection makes no progress");
  }

  /**
   * The offset-store side of the same read. Also pins that a recovered read resumes reporting the
   * CURRENT lag rather than staying pinned to the held value.
   */
  @Test
  void lagGaugeRecoversAfterAFailedOffsetStoreRead() {
    var metrics = new RecordingStreamRuneMetrics();
    var withMetrics = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);
    withMetrics.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(200);
    stubLastOffset("orders", 190);
    assertEquals(10, metrics.sampleLagGauge("orders"));

    doThrow(new IllegalStateException("connection pool exhausted"))
        .when(offsetStore)
        .getLastOffset(ProjectionName.of("orders"));
    assertEquals(10, metrics.sampleLagGauge("orders"), "held, not reset to 0");

    // doReturn(...) rather than when(...): the current stub throws, so evaluating the argument to
    // when() would fire it.
    doReturn(GlobalOffset.of(120)).when(offsetStore).getLastOffset(ProjectionName.of("orders"));
    assertEquals(80, metrics.sampleLagGauge("orders"), "a recovered read reports the current lag");
  }

  // ==== A dead LISTEN/NOTIFY push path must be visible in subscription health ====

  /**
   * A lifecycle whose push-path status is controllable, standing in for HybridEventSubscription.
   */
  private interface PushAwareLifecycle extends SubscriptionLifecycle, PushHealthAware {}

  private PushAwareLifecycle pushAware(
      SubscriptionLifecycleState state, PushHealthAware.PushPathStatus pushStatus) {
    var sub = mock(PushAwareLifecycle.class);
    when(sub.state()).thenReturn(state);
    when(sub.pushPathStatus()).thenReturn(pushStatus);
    return sub;
  }

  @Test
  void aDeadPushPathDegradesAnOtherwiseHealthySubscription() {
    contributor.register(
        "orders",
        pushAware(SubscriptionLifecycleState.RUNNING, PushHealthAware.PushPathStatus.DEAD));
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(
        SubscriptionHealth.Status.DEGRADED,
        contributor.health().getFirst().status(),
        "delivery has silently fallen back to the poll interval — that is not UP");
  }

  @Test
  void aLivePushPathLeavesTheSubscriptionUp() {
    contributor.register(
        "orders",
        pushAware(SubscriptionLifecycleState.RUNNING, PushHealthAware.PushPathStatus.LIVE));
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(SubscriptionHealth.Status.UP, contributor.health().getFirst().status());
  }

  @Test
  void aSubscriptionWithNoPushPathConfiguredIsNotDegraded() {
    // A pollingOnly deployment has no push path to lose; reporting it degraded forever would train
    // operators to ignore the signal.
    contributor.register(
        "orders",
        pushAware(
            SubscriptionLifecycleState.RUNNING, PushHealthAware.PushPathStatus.NOT_CONFIGURED));
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(SubscriptionHealth.Status.UP, contributor.health().getFirst().status());
  }

  @Test
  void aDeadPushPathNeverMasksADownVerdict() {
    // A stopped subscription's listener is legitimately down too — the lifecycle state is the
    // stronger, more urgent signal and must survive.
    contributor.register(
        "orders",
        pushAware(SubscriptionLifecycleState.STOPPED, PushHealthAware.PushPathStatus.DEAD));
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(SubscriptionHealth.Status.DOWN, contributor.health().getFirst().status());
  }

  @Test
  void aFailingPushHealthProbeDoesNotBreakTheHealthEndpoint() {
    var sub = mock(PushAwareLifecycle.class);
    when(sub.state()).thenReturn(SubscriptionLifecycleState.RUNNING);
    when(sub.pushPathStatus()).thenThrow(new IllegalStateException("probe blew up"));
    contributor.register("orders", sub);
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(
        SubscriptionHealth.Status.UP,
        contributor.health().getFirst().status(),
        "a subscription's own probe must never take out health reporting for everything else");
  }

  @Test
  void aSubscriptionWithoutThePushCapabilityIsUnaffected() {
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(10);
    stubLastOffset("orders", 10);

    assertEquals(SubscriptionHealth.Status.UP, contributor.health().getFirst().status());
  }

  @Test
  void defaultContributorRecordsNoLagMetric() {
    // Behaviour unchanged when no StreamRuneMetrics is supplied (NOOP default).
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));
    stubGlobalHead(50);
    stubLastOffset("orders", 45);

    var h = contributor.health().getFirst();
    assertEquals(5, h.lag(), "lag still computed for the health report");
  }

  // ── A checkpoint ahead of the stream head is reported, not clamped away ─────────────────────

  @Test
  void checkpointAheadOfTheHead_readsDegradedWithLagZero_andWarnsOncePerEpisode() {
    // A restore that put the event table back to offset 800 while a checkpoint still says 1000
    // silently skips the events later appended at 801..1000. Lag used to clamp the negative gap to
    // 0 and report UP, hiding the one condition a consistent database can never produce.
    stubGlobalHead(800);
    stubLastOffset("orders", 1000);
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(SubscriptionHealthContributor.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    List<SubscriptionHealth> first;
    try {
      first = contributor.health();
      contributor.health();
      contributor.currentLag("orders");
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    var h = first.getFirst();
    assertEquals(0, h.lag(), "the lag value stays non-negative");
    assertEquals(SubscriptionHealth.Status.DEGRADED, h.status());
    var warns =
        appender.list.stream()
            .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .filter(m -> m.contains("ahead of the event stream head"))
            .toList();
    assertEquals(1, warns.size(), "one WARN per episode, not one per scrape: " + warns);
    assertTrue(
        warns.getFirst().contains("orders")
            && warns.getFirst().contains("1000")
            && warns.getFirst().contains("800"),
        "the WARN names the subscription and both offsets: " + warns.getFirst());

    // The episode ends once the head passes the checkpoint again; a later one warns again.
    stubGlobalHead(1000);
    assertEquals(SubscriptionHealth.Status.UP, contributor.health().getFirst().status());
    stubGlobalHead(900);
    appender.list.clear();
    appender.start();
    logger.addAppender(appender);
    try {
      contributor.health();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    assertEquals(
        1,
        appender.list.stream()
            .filter(e -> e.getFormattedMessage().contains("ahead of the event stream head"))
            .count(),
        "a new episode warns again");
  }

  @Test
  void aCheckpointThatMovesBetweenTheTwoReads_isNotReportedAhead() {
    // The checkpoint is read before the head. Read the other way round, a batch that commits
    // between the reads (appends to 20, checkpoint to 20) pairs an old head (10) with a newer
    // checkpoint (20) and would look like a checkpoint ahead of the stream.
    var head = new java.util.concurrent.atomic.AtomicLong(10);
    when(eventStore.lastGlobalOffset()).thenAnswer(inv -> GlobalOffset.of(head.get()));
    when(offsetStore.getLastOffset(ProjectionName.of("orders")))
        .thenAnswer(
            inv -> {
              head.set(20); // the concurrent batch commits while the health sample is taken
              return GlobalOffset.of(20);
            });
    contributor.register("orders", mockSubscription(SubscriptionLifecycleState.RUNNING));

    var h = contributor.health().getFirst();

    assertEquals(0, h.lag());
    assertEquals(SubscriptionHealth.Status.UP, h.status());
  }
}
