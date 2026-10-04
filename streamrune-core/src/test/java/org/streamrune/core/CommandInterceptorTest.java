package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;

class CommandInterceptorTest {

  record TestCmd() implements Command {}

  private CommandInterceptor.CommandContext ctx() {
    return new CommandInterceptor.CommandContext(
        new TestCmd(),
        "CommandType",
        CommandId.of("cmd_1"),
        AggregateType.of("order"),
        AggregateId.of("agg_1"),
        null,
        Instant.now());
  }

  @Test
  void commandContext_streamId_composesTheTwoParts_andIsNullWhenEitherIsMissing() {
    var type = AggregateType.of("order");
    var id = AggregateId.of("o-1");
    var ctx =
        new CommandInterceptor.CommandContext(
            null, "PlaceOrder", CommandId.of("c-1"), type, id, null, Instant.EPOCH);
    assertEquals(StreamId.of(type, id), ctx.streamId());
    assertNull(
        new CommandInterceptor.CommandContext(null, "Q", null, null, null, null, Instant.EPOCH)
            .streamId());
    assertNull(
        new CommandInterceptor.CommandContext(null, "Q", null, type, null, null, Instant.EPOCH)
            .streamId());
    assertNull(
        new CommandInterceptor.CommandContext(null, "Q", null, null, id, null, Instant.EPOCH)
            .streamId());
  }

  @Test
  void noopReturnsTrueOnBefore() {
    CommandInterceptor i = CommandInterceptor.noop();
    assertTrue(i.before(ctx()));
    assertDoesNotThrow(() -> i.after(ctx()));
    assertDoesNotThrow(() -> i.onError(ctx(), new RuntimeException()));
  }

  @Test
  void composeCallsBothBeforeInOrder() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first = new RecordingInterceptor(calls, "a", true);
    CommandInterceptor second = new RecordingInterceptor(calls, "b", true);

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    assertTrue(composed.before(ctx()));
    assertEquals(List.of("a-before", "b-before"), calls);
  }

  @Test
  void composeShortCircuitsWhenFirstReturnsFalse() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first = new RecordingInterceptor(calls, "a", false);
    CommandInterceptor second = new RecordingInterceptor(calls, "b", true);

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    assertFalse(composed.before(ctx()));
    assertEquals(List.of("a-before"), calls);
  }

  @Test
  void composeCallsAfterInReverseOrder() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor composed =
        CommandInterceptor.compose(
            new RecordingInterceptor(calls, "a", true), new RecordingInterceptor(calls, "b", true));

    composed.after(ctx());
    assertEquals(List.of("b-after", "a-after"), calls);
  }

  @Test
  void composeCallsOnErrorInReverseOrder() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor composed =
        CommandInterceptor.compose(
            new RecordingInterceptor(calls, "a", true), new RecordingInterceptor(calls, "b", true));

    composed.onError(ctx(), new RuntimeException("x"));
    assertEquals(List.of("b-onError", "a-onError"), calls);
  }

  @Test
  void composeShortCircuitsWhenSecondReturnsFalse() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first = new RecordingInterceptor(calls, "a", true);
    CommandInterceptor second = new RecordingInterceptor(calls, "b", false);

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    assertFalse(composed.before(ctx()));
    assertEquals(List.of("a-before", "b-before"), calls);
  }

  @Test
  void composeNotifiesFirstOnErrorWhenSecondBeforeThrows() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first = new RecordingInterceptor(calls, "a", true);
    CommandInterceptor second =
        new RecordingInterceptor(calls, "b", true) {
          @Override
          public boolean before(CommandContext ctx) {
            calls.add("b-before");
            throw new IllegalStateException("b before failed");
          }
        };

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    var thrown = assertThrows(IllegalStateException.class, () -> composed.before(ctx()));
    assertEquals("b before failed", thrown.getMessage());
    assertEquals(
        List.of("a-before", "b-before", "a-onError"),
        calls,
        "first.before() completed, so first must be notified of second's failure");
  }

  @Test
  void composeSuppressesOnErrorFailureWhenSecondBeforeThrows() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first =
        new RecordingInterceptor(calls, "a", true) {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            calls.add("a-onError");
            throw new IllegalStateException("a onError failed");
          }
        };
    CommandInterceptor second =
        new RecordingInterceptor(calls, "b", true) {
          @Override
          public boolean before(CommandContext ctx) {
            calls.add("b-before");
            throw new IllegalStateException("b before failed");
          }
        };

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    var thrown = assertThrows(IllegalStateException.class, () -> composed.before(ctx()));
    assertEquals("b before failed", thrown.getMessage(), "original failure propagates");
    assertEquals(1, thrown.getSuppressed().length);
    assertEquals("a onError failed", thrown.getSuppressed()[0].getMessage());
  }

  @Test
  void composeAfterRunsRemainingMemberWhenFirstCalledThrows() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first = new RecordingInterceptor(calls, "a", true);
    CommandInterceptor second =
        new RecordingInterceptor(calls, "b", true) {
          @Override
          public void after(CommandContext ctx) {
            calls.add("b-after");
            throw new IllegalStateException("b after failed");
          }
        };

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    var thrown = assertThrows(IllegalStateException.class, () -> composed.after(ctx()));
    assertEquals("b after failed", thrown.getMessage());
    assertEquals(
        List.of("b-after", "a-after"), calls, "a throwing member must not starve the other");
  }

  @Test
  void composeOnErrorRunsRemainingMemberAndSuppressesSecondaryFailure() {
    List<String> calls = new ArrayList<>();
    CommandInterceptor first =
        new RecordingInterceptor(calls, "a", true) {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            calls.add("a-onError");
            throw new IllegalStateException("a onError failed");
          }
        };
    CommandInterceptor second =
        new RecordingInterceptor(calls, "b", true) {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            calls.add("b-onError");
            throw new IllegalStateException("b onError failed");
          }
        };

    CommandInterceptor composed = CommandInterceptor.compose(first, second);
    var thrown =
        assertThrows(
            IllegalStateException.class, () -> composed.onError(ctx(), new RuntimeException("x")));
    assertEquals("b onError failed", thrown.getMessage(), "first-called member's failure wins");
    assertEquals(List.of("b-onError", "a-onError"), calls);
    assertEquals(1, thrown.getSuppressed().length);
    assertEquals("a onError failed", thrown.getSuppressed()[0].getMessage());
  }

  private static class RecordingInterceptor implements CommandInterceptor {
    private final List<String> calls;
    private final String tag;
    private final boolean allowed;

    RecordingInterceptor(List<String> calls, String tag, boolean allowed) {
      this.calls = calls;
      this.tag = tag;
      this.allowed = allowed;
    }

    @Override
    public boolean before(CommandContext ctx) {
      calls.add(tag + "-before");
      return allowed;
    }

    @Override
    public void after(CommandContext ctx) {
      calls.add(tag + "-after");
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      calls.add(tag + "-onError");
    }
  }
}
