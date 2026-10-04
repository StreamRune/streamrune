package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryOutboxStore;

class OutboxFailedReplayerTest {

  InMemoryOutboxStore store;

  @BeforeEach
  void setUp() {
    store = afStore();
  }

  // These pins cover the replayer's enumeration and CAS mechanics on null-aggregate fixtures, so
  // the store is AVAILABILITY_FIRST; the strict default would reject every null-aggregate save.
  // Ordering-mode behaviour is pinned in OutboxStoreContract and in the strict-specific
  // tests below, which construct strictStore() explicitly.
  private static InMemoryOutboxStore afStore() {
    return new InMemoryOutboxStore(
        Clock.systemUTC(),
        InMemoryOutboxStore.DEFAULT_CLAIM_LEASE,
        OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  private static InMemoryOutboxStore strictStore() {
    return new InMemoryOutboxStore(); // strict by default
  }

  /**
   * Records every recordOutboxReplayed(rows) call and counts recordOutboxSkipped(); all other
   * methods are no-ops.
   */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final List<Long> replayed = new ArrayList<>();
    int skipped;

    @Override
    public void recordOutboxReplayed(long rows) {
      replayed.add(rows);
    }

    @Override
    public void recordOutboxSkipped() {
      skipped++;
    }
  }

  /** Saves {@code n} null-aggregate entries, claims all in one batch, then marks each FAILED. */
  private void seedFailed(String prefix, int n) {
    for (int i = 1; i <= n; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of(prefix + i), "{}", "Event"));
    }
    assertThat(store.loadPending(1000)).hasSize(n);
    for (int i = 1; i <= n; i++) {
      assertThat(store.markFailed(OutboxEntryId.of(prefix + i), i, "boom", store.claimedBy()))
          .isTrue();
    }
  }

  // ── logback capture for the OutboxFailedReplayer logger ─────────────────────────────────────

  /**
   * Attaches a logback {@code ListAppender} to the {@code OutboxFailedReplayer} logger so a test
   * can assert on the skip WARN's actual text — its wording IS the operator-facing contract.
   * Callers must detach via {@link #detachReplayerLogs}.
   */
  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
      captureReplayerLogs() {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(OutboxFailedReplayer.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachReplayerLogs(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(OutboxFailedReplayer.class))
        .detachAppender(appender);
  }

  /** The formatted WARN messages the appender captured. */
  private static List<String> warnLines(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void replayFailed_resetsOldestFirst_boundedByMax_clearsAttemptsAndError() {
    seedFailed("f", 5); // f1..f5 FAILED, increasing seq
    var metrics = new RecordingMetrics();
    var replayer = new OutboxFailedReplayer(store, metrics);

    int reset = replayer.replayFailed(3); // reset the 3 OLDEST only
    assertThat(reset).isEqualTo(3);
    assertThat(metrics.replayed).containsExactly(3L);

    // f1,f2,f3 → PENDING; f4,f5 stay FAILED.
    var stillFailed =
        store.findByStatus(OutboxStatus.FAILED, 10).stream().map(e -> e.id().value()).toList();
    assertThat(stillFailed).containsExactly("f4", "f5");

    var pending =
        store.findByStatus(OutboxStatus.PENDING, 10).stream().map(e -> e.id().value()).toList();
    assertThat(pending).containsExactly("f1", "f2", "f3");

    // The reset entries had attempts/error cleared.
    var f1 =
        store.all().stream().filter(e -> e.id().value().equals("f1")).findFirst().orElseThrow();
    assertThat(f1.attempts()).isZero();
    assertThat(f1.lastError()).isNull();
    assertThat(f1.nextRetryAt()).isNull();
  }

  @Test
  void replayFailed_withNoFailedEntries_resetsZero_recordsZero() {
    var metrics = new RecordingMetrics();
    var replayer = new OutboxFailedReplayer(store, metrics);
    assertThat(replayer.replayFailed(10)).isZero();
    assertThat(metrics.replayed).containsExactly(0L);
  }

  @Test
  void replayFailed_survivesThrowingReplayedMetric() {
    // recordOutboxReplayed ran bare. The resets already committed by the time it's called,
    // so a throwing metrics backend must not turn a successful replay into a thrown exception —
    // the caller would see an error for a call that actually succeeded.
    seedFailed("t", 2);
    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordOutboxReplayed(long rows) {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var replayer = new OutboxFailedReplayer(store, throwing);

    int reset = assertDoesNotThrow(() -> replayer.replayFailed(10));
    assertThat(reset).isEqualTo(2);
  }

  @Test
  void replayFailed_resetAggregateEntry_isRedeliveredInSeqOrder() {
    // Two entries of aggregate A on a STRICT channel: a1 (seq1), a2 (seq2). Fail a1 at the head.
    var strict = strictStore();
    var a = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-A"));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("a1"), "{}", "Event", a));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("a2"), "{}", "Event", a));

    // Claim a1 (the head) and mark it FAILED.
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("a1");
    assertThat(strict.markFailed(OutboxEntryId.of("a1"), 3, "boom", strict.claimedBy())).isTrue();
    // The FAILED head blocks its aggregate: a2 is NOT claimable. (The availability-first rule this
    // design removes let a2 become the eligible head here and leapfrog a1.)
    assertThat(strict.loadPending(10)).as("a2 stays blocked behind the FAILED head a1").isEmpty();

    // Replay a1 → PENDING at its original position (same seq): it is the aggregate's head again
    // and is delivered BEFORE its successor a2.
    new OutboxFailedReplayer(strict, StreamRuneMetrics.NOOP).replayFailed(10);
    assertThat(strict.loadPending(10))
        .extracting(e -> e.id().value())
        .as("reset aggregate entry a1 is redelivered before its successor a2")
        .containsExactly("a1");
    assertThat(strict.markDelivered(OutboxEntryId.of("a1"), strict.claimedBy())).isTrue();
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("a2");
  }

  @Test
  void constructor_rejectsNulls() {
    assertThrows(
        NullPointerException.class, () -> new OutboxFailedReplayer(null, StreamRuneMetrics.NOOP));
    assertThrows(NullPointerException.class, () -> new OutboxFailedReplayer(store, null));
  }

  @Test
  void replayFailed_rejectsNonPositiveMax() {
    var replayer = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP);
    assertThrows(IllegalArgumentException.class, () -> replayer.replayFailed(0));
  }

  // ── replay(id), skip(id, by, reason), bulk skipFailed(max, by, reason) ─────────────────

  @Test
  void replay_single_outcomes() {
    var strict = strictStore();
    var a = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-R"));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("r1"), "{}", "Event", a));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("r2"), "{}", "Event", a));
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("r1");
    assertThat(strict.markFailed(OutboxEntryId.of("r1"), 10, "poison", strict.claimedBy()))
        .isTrue();
    var metrics = new RecordingMetrics();
    var replayer = new OutboxFailedReplayer(strict, metrics);

    assertThat(replayer.replay(OutboxEntryId.of("absent")))
        .isEqualTo(OutboxFailedReplayer.ReplayOutcome.NOT_FOUND);
    assertThat(replayer.replay(OutboxEntryId.of("r2")))
        .isEqualTo(OutboxFailedReplayer.ReplayOutcome.NOT_FAILED);
    assertThat(replayer.replay(OutboxEntryId.of("r1")))
        .isEqualTo(OutboxFailedReplayer.ReplayOutcome.REPLAYED);
    assertThat(metrics.replayed).containsExactly(1L);
    // A rerun finds the row PENDING and does not reset it twice.
    assertThat(replayer.replay(OutboxEntryId.of("r1")))
        .isEqualTo(OutboxFailedReplayer.ReplayOutcome.NOT_FAILED);
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("r1");
  }

  @Test
  void skip_outcomes_andAuditOnTheRow() {
    var strict = strictStore();
    var a = StreamId.of(AggregateType.of("agg"), AggregateId.of("agg-S"));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("s1"), "{}", "Event", a));
    strict.save(OutboxEntry.pending(OutboxEntryId.of("s2"), "{}", "Event", a));
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("s1");
    assertThat(strict.markFailed(OutboxEntryId.of("s1"), 10, "poison", strict.claimedBy()))
        .isTrue();
    var metrics = new RecordingMetrics();
    var replayer = new OutboxFailedReplayer(strict, metrics);

    assertThat(replayer.skip(OutboxEntryId.of("absent"), "ops", "r"))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.NOT_FOUND);
    assertThat(replayer.skip(OutboxEntryId.of("s2"), "ops", "r"))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.NOT_FAILED);
    assertThat(replayer.skip(OutboxEntryId.of("s1"), "ops", "ticket OPS-42: schema rejected"))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.SKIPPED);
    assertThat(metrics.skipped).isEqualTo(1);
    var row = strict.findById(OutboxEntryId.of("s1")).orElseThrow();
    assertThat(row.status()).isEqualTo(OutboxStatus.SKIPPED);
    assertThat(row.skip().skippedBy()).isEqualTo("ops");
    assertThat(row.skip().reason()).isEqualTo("ticket OPS-42: schema rejected");
    // A rerun finds the row SKIPPED; nothing is written twice, the counter is not bumped.
    assertThat(replayer.skip(OutboxEntryId.of("s1"), "ops", "again"))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.NOT_FAILED);
    assertThat(metrics.skipped).isEqualTo(1);
    assertThat(strict.loadPending(10)).extracting(e -> e.id().value()).containsExactly("s2");
  }

  @Test
  void skip_rejectsBlankByOrReason_andNulls() {
    var replayer = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP);
    var id = OutboxEntryId.of("x");
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, " ", "reason"));
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, "ops", ""));
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, null, "reason"));
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, "ops", null));
    assertThrows(NullPointerException.class, () -> replayer.skip(null, "ops", "reason"));
    // Blankness is judged on what would be persisted: the sanitizer removes a zero-width space
    // (Cf) and turns a BEL (C0) into one space, neither of which String.isBlank() sees as blank.
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, "ops", "\u200B"));
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, "\u0007", "reason"));
    assertThrows(
        IllegalArgumentException.class, () -> replayer.skipFailed(5, "ops", "\u200B\u200B"));
    // ... and after the cap: a value whose first MAX characters are blank persists nothing useful.
    String blankHead = " ".repeat(OutboxFailedReplayer.MAX_SKIPPED_BY_LENGTH) + "x";
    assertThrows(IllegalArgumentException.class, () -> replayer.skip(id, blankHead, "reason"));
  }

  @Test
  void skip_sanitizesAndCapsByAndReasonBeforePersisting() {
    seedFailed("c", 1);
    var replayer = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP);
    String longReason = "r".repeat(OutboxFailedReplayer.MAX_REASON_LENGTH + 500);
    String by = "ops\nFAKE\tLINE" + "b".repeat(OutboxFailedReplayer.MAX_SKIPPED_BY_LENGTH);

    assertThat(replayer.skip(OutboxEntryId.of("c1"), by, "line1\nline2 " + longReason))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.SKIPPED);

    var skip = store.findById(OutboxEntryId.of("c1")).orElseThrow().skip();
    assertThat(skip.skippedBy()).doesNotContain("\n").doesNotContain("\t");
    assertThat(skip.skippedBy().length())
        .isLessThanOrEqualTo(OutboxFailedReplayer.MAX_SKIPPED_BY_LENGTH);
    assertThat(skip.reason()).doesNotContain("\n");
    assertThat(skip.reason().length()).isLessThanOrEqualTo(OutboxFailedReplayer.MAX_REASON_LENGTH);
    assertThat(skip.reason())
        .isEqualTo(
            LogSanitizer.sanitizeFreeText("line1\nline2 " + longReason)
                .substring(
                    0,
                    Math.min(
                        OutboxFailedReplayer.MAX_REASON_LENGTH,
                        LogSanitizer.sanitizeFreeText("line1\nline2 " + longReason).length())));
  }

  @Test
  void skip_capsWithoutSplittingASurrogatePair() {
    seedFailed("s", 1);
    var replayer = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP);
    String head = "b".repeat(OutboxFailedReplayer.MAX_SKIPPED_BY_LENGTH - 1);
    // U+1F600 is two UTF-16 units: its high surrogate is the cap's last unit.
    String by = head + "\uD83D\uDE00";

    assertThat(replayer.skip(OutboxEntryId.of("s1"), by, "reason"))
        .isEqualTo(OutboxFailedReplayer.SkipOutcome.SKIPPED);

    String persisted = store.findById(OutboxEntryId.of("s1")).orElseThrow().skip().skippedBy();
    assertThat(persisted).isEqualTo(head);
    assertThat(Character.isHighSurrogate(persisted.charAt(persisted.length() - 1))).isFalse();
  }

  @Test
  void skip_recordsMetric_andSurvivesThrowingMetric() {
    seedFailed("m", 1);
    StreamRuneMetrics throwing =
        new StreamRuneMetrics() {
          @Override
          public void recordOutboxSkipped() {
            throw new IllegalStateException("simulated metrics backend failure");
          }
        };
    var replayer = new OutboxFailedReplayer(store, throwing);
    var outcome = assertDoesNotThrow(() -> replayer.skip(OutboxEntryId.of("m1"), "ops", "r"));
    assertThat(outcome).isEqualTo(OutboxFailedReplayer.SkipOutcome.SKIPPED);
    assertThat(store.findById(OutboxEntryId.of("m1")).orElseThrow().status())
        .isEqualTo(OutboxStatus.SKIPPED);
  }

  @Test
  void skip_logIsConditionalOnHeadness_neverClaimsSuccessorsReleased() {
    // The replayer does not know whether the skipped row was its stream's head (it reads no seq),
    // so the WARN says "If it was its stream's blocking head" — never "released".
    seedFailed("l", 1);
    var appender = captureReplayerLogs();
    try {
      new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP)
          .skip(OutboxEntryId.of("l1"), "ops", "poison");
      var warns = warnLines(appender);
      assertThat(warns).hasSize(1);
      assertThat(warns.get(0))
          .contains("SKIPPED by ops")
          .contains("this entry will never be delivered")
          .contains("the downstream did NOT receive this change")
          .contains("If it was its stream's blocking head")
          .doesNotContain("successors released");
    } finally {
      detachReplayerLogs(appender);
    }
  }

  @Test
  void skipFailed_bulk_oldestFirst_oneCasPerRow_countsOnlyTrue() {
    seedFailed("b", 5); // b1..b5 FAILED, increasing seq
    var casCalls = new ArrayList<String>();
    var spy =
        new org.streamrune.test.ForwardingOutboxStore(store) {
          @Override
          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            casCalls.add(id.value());
            return super.skipFailed(id, skippedBy, reason);
          }
        };
    var metrics = new RecordingMetrics();
    var replayer = new OutboxFailedReplayer(spy, metrics);

    assertThat(replayer.skipFailed(3, "ops", "consumer retired")).isEqualTo(3);

    assertThat(casCalls).containsExactly("b1", "b2", "b3");
    assertThat(metrics.skipped).isEqualTo(3);
    assertThat(store.findByStatus(OutboxStatus.FAILED, 10))
        .extracting(e -> e.id().value())
        .containsExactly("b4", "b5");
    assertThat(store.findByStatus(OutboxStatus.SKIPPED, 10))
        .allSatisfy(
            e -> {
              assertThat(e.skip().skippedBy()).isEqualTo("ops");
              assertThat(e.skip().reason()).isEqualTo("consumer retired");
            });
  }

  @Test
  void skipFailed_bulk_rerunAfterPartialRun_skipsOnlyRowsStillFailed() {
    seedFailed("p", 4); // p1..p4
    var crashAfterTwo =
        new org.streamrune.test.ForwardingOutboxStore(store) {
          int writes;

          @Override
          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            if (++writes == 3) {
              throw new IllegalStateException("crash after the 2nd committed skip");
            }
            return super.skipFailed(id, skippedBy, reason);
          }
        };
    var first = new OutboxFailedReplayer(crashAfterTwo, StreamRuneMetrics.NOOP);
    assertThrows(IllegalStateException.class, () -> first.skipFailed(10, "ops", "r"));
    // Crash after k=2 skips: p1, p2 SKIPPED with full audit (each write is one statement), p3, p4
    // FAILED; the counter/WARN of the first run are lost — the rows are the record.
    assertThat(store.findByStatus(OutboxStatus.SKIPPED, 10))
        .extracting(e -> e.id().value())
        .containsExactly("p1", "p2");
    // Someone replays p3 in between: it is PENDING, not enumerated, not touched by the rerun.
    assertThat(store.resetFailedToPending(OutboxEntryId.of("p3"))).isTrue();

    var metrics = new RecordingMetrics();
    int rerun = new OutboxFailedReplayer(store, metrics).skipFailed(10, "ops", "r");

    assertThat(rerun).as("only p4 was still FAILED").isEqualTo(1);
    assertThat(metrics.skipped).isEqualTo(1);
    assertThat(store.findByStatus(OutboxStatus.SKIPPED, 10))
        .extracting(e -> e.id().value())
        .containsExactly("p1", "p2", "p4");
    assertThat(store.findById(OutboxEntryId.of("p3")).orElseThrow().status())
        .isEqualTo(OutboxStatus.PENDING);
  }

  @Test
  void skipFailed_bulk_rejectsBlankByOrReason_andMaxBelowOne() {
    var replayer = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP);
    assertThrows(IllegalArgumentException.class, () -> replayer.skipFailed(0, "ops", "r"));
    assertThrows(IllegalArgumentException.class, () -> replayer.skipFailed(1, "", "r"));
    assertThrows(IllegalArgumentException.class, () -> replayer.skipFailed(1, "ops", "  "));
  }

  @Test
  void skipFailed_bulk_summaryWarn_namesCounts_andThatNothingWasDelivered() {
    seedFailed("w", 2);
    var appender = captureReplayerLogs();
    try {
      new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP).skipFailed(5, "ops", "retired");
      var warns = warnLines(appender);
      assertThat(warns).hasSize(1);
      assertThat(warns.get(0))
          .contains("Outbox bulk skip: 2 of 2 enumerated FAILED entries SKIPPED by ops")
          .contains("(maxEntries=5)")
          .contains("none of them was delivered downstream");
    } finally {
      detachReplayerLogs(appender);
    }
  }
}
