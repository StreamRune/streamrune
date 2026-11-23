package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

class SagaParallelismDefaultTest {

  private static final EventTypeRegistry TYPES = SimpleEventTypeRegistry.builder().build();
  private static final String TYPE = SagaStepLockFixture.TYPE.value();

  @Test
  void theDefaultIsFourFromTheDefaultSource() {
    SagaRunner<State> runner = runner(b -> b);

    assertThat(SagaParallelismDefault.effectiveOf(runner)).isEqualTo(4);
    assertThat(SagaParallelismDefault.sourceOf(runner)).isEqualTo(SagaParallelismSource.DEFAULT);
  }

  @Test
  void aSubscriptionPropertyLaterRemovedFallsBackToTheBuilder() {
    SagaRunner<State> runner = runner(b -> b.parallelism(2));
    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of("orders", 16)));
    runner.bindSubscription(SubscriptionName.of("orders"));
    assertThat(runner.parallelism()).isEqualTo(16);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);

    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of()));

    assertThat(runner.parallelism()).isEqualTo(2);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.BUILDER);
  }

  @Test
  void theGlobalPropertyReachesARunnerThatNamedNothing() {
    SagaRunner<State> runner = runner(b -> b);

    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of()));

    assertThat(runner.parallelism()).isEqualTo(8);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
  }

  @Test
  void theBuilderWinsOverTheGlobalProperty() {
    SagaRunner<State> runner = runner(b -> b.parallelism(2));

    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of()));

    assertThat(runner.parallelism()).isEqualTo(2);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.BUILDER);
  }

  @Test
  void theSubscriptionPropertyWinsOverTheBuilder_settingsFirst() {
    SagaRunner<State> runner = runner(b -> b.parallelism(2));
    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of("orders", 16)));
    assertThat(runner.parallelism()).isEqualTo(2);

    runner.bindSubscription(SubscriptionName.of("orders"));

    assertThat(runner.parallelism()).isEqualTo(16);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
  }

  @Test
  void theSubscriptionPropertyWinsOverTheBuilder_bindFirst() {
    SagaRunner<State> runner = runner(b -> b.parallelism(2));
    runner.bindSubscription(SubscriptionName.of("orders"));
    assertThat(runner.parallelism()).isEqualTo(2);

    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of("orders", 16)));

    assertThat(runner.parallelism()).isEqualTo(16);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.SUBSCRIPTION_PROPERTY);
  }

  @Test
  void aNameWithADotOrUpperCaseIsMatchedExactly() {
    SagaRunner<State> runner = runner(b -> b);
    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of("Orders.v2", 3)));

    runner.bindSubscription(SubscriptionName.of("orders.v2"));
    assertThat(runner.parallelism()).isEqualTo(8);

    runner.bindSubscription(SubscriptionName.of("Orders.v2"));
    assertThat(runner.parallelism()).isEqualTo(3);
  }

  @Test
  void bindingTheSameNameTwiceIsIdempotent() {
    SagaRunner<State> runner = runner(b -> b);

    runner.bindSubscription(SubscriptionName.of("orders"));
    runner.bindSubscription(SubscriptionName.of("orders"));

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("orders"));
  }

  @Test
  void aReleasedNameLeavesTheBoundNames_andTheParallelismIsResolvedWithoutIt() {
    SagaRunner<State> runner = runner(b -> b);
    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("a", 2)));
    runner.bindSubscription(SubscriptionName.of("a"));
    runner.bindSubscription(SubscriptionName.of("b"));
    assertThat(runner.parallelism()).isEqualTo(2);

    runner.releaseSubscription(SubscriptionName.of("a"));

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("b"));
    assertThat(runner.parallelismSnapshot().concurrentPages()).isEqualTo(1);
    assertThat(runner.parallelism()).isEqualTo(4);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
  }

  @Test
  void aNameBoundTwiceStaysUntilBothAreReleased_andAnUnknownReleaseIsANoOp() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("orders"));
    runner.bindSubscription(SubscriptionName.of("orders"));

    runner.releaseSubscription(SubscriptionName.of("orders"));
    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("orders"));
    runner.releaseSubscription(SubscriptionName.of("orders"));
    assertThat(runner.boundSubscriptionNames()).isEmpty();
    runner.releaseSubscription(SubscriptionName.of("orders"));
    runner.releaseSubscription(SubscriptionName.of("never-bound"));
    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  @Test
  void aClosedPollingSubscriptionReleasesItsBind_soARebuiltOneUnderAnotherNameCountsOnce() {
    SagaRunner<State> runner = runner(b -> b);
    PollingEventSubscription first = polling(runner, "orders");

    first.close();
    PollingEventSubscription second = polling(runner, "orders.v2");

    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("orders.v2"));
    assertThat(runner.parallelismSnapshot().concurrentPages()).isEqualTo(1);
    first.close();
    assertThat(runner.boundSubscriptionNames())
        .as("a second close releases nothing more")
        .containsExactly(SubscriptionName.of("orders.v2"));
    second.close();
    assertThat(runner.boundSubscriptionNames()).isEmpty();
  }

  private static PollingEventSubscription polling(SagaRunner<State> runner, String name) {
    return PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of(name))
        .eventStore(new InMemoryEventStore(TYPES))
        .offsetStore(new InMemoryOffsetStore())
        .sagaRunner(runner)
        .build();
  }

  @Test
  void twoBoundNamesWithDifferentValuesAreRefused() {
    SagaRunner<State> runner = runner(b -> b);
    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("a", 2, "b", 3)));
    runner.bindSubscription(SubscriptionName.of("a"));

    assertThatThrownBy(() -> runner.bindSubscription(SubscriptionName.of("b")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Saga type "
                + TYPE
                + " is bound to the subscriptions 'a' and 'b', which set different parallelism"
                + " (2 and 3): set one value");
    assertThat(runner.boundSubscriptionNames()).containsExactly(SubscriptionName.of("a"));
    assertThat(runner.parallelism()).isEqualTo(2);
  }

  @Test
  void twoBoundNamesOneWithAValueTakeIt() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("a"));
    runner.bindSubscription(SubscriptionName.of("b"));

    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("b", 7)));

    assertThat(runner.parallelism()).isEqualTo(7);
  }

  @Test
  void twoBoundNamesWithTheSameValueAreAccepted() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("a"));
    runner.bindSubscription(SubscriptionName.of("b"));

    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("a", 5, "b", 5)));

    assertThat(runner.parallelism()).isEqualTo(5);
  }

  @Test
  void anUnknownSubscriptionNameIsRefusedListingTheKnownNames() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("orders"));
    runner.bindSubscription(SubscriptionName.of("billing"));
    SagaParallelismSettings settings =
        new SagaParallelismSettings(4, Map.of("zeta", 2, "alpha", 3, "orders", 5));

    assertThatThrownBy(
            () -> SagaParallelismDefault.requireKnownSubscriptions(List.of(runner), settings))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.saga.subscriptions.<name>.parallelism names a subscription no saga runner"
                + " is bound to: [alpha, zeta]. Subscriptions bound to a saga runner: [billing,"
                + " orders]. A saga's subscription is bound when it is built with"
                + " .sagaRunner(runner) before startup ends.");
    SagaParallelismDefault.requireKnownSubscriptions(
        List.of(runner), new SagaParallelismSettings(4, Map.of("orders", 5)));
  }

  @Test
  void settingsOutsideOneTo64AreRefusedNamingTheProperty() {
    for (int bad : new int[] {0, 65}) {
      assertThatThrownBy(() -> new SagaParallelismSettings(bad, Map.of()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("streamrune.saga.parallelism must be between 1 and 64, got: " + bad);
      assertThatThrownBy(() -> new SagaParallelismSettings(4, Map.of("orders", bad)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "streamrune.saga.subscriptions.orders.parallelism must be between 1 and 64, got: "
                  + bad);
    }
  }

  @Test
  void theSettingsCopyTheirMap() {
    Map<String, Integer> source = new java.util.HashMap<>(Map.of("a", 2));
    SagaParallelismSettings settings = new SagaParallelismSettings(4, source);
    source.put("b", 3);

    assertThat(settings.bySubscription()).containsOnlyKeys("a");
    assertThatThrownBy(() -> settings.bySubscription().put("c", 1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void theStartupLineNamesValueSourceAndBudget() {
    SagaRunner<State> bySubscription = runner(b -> b);
    bySubscription.bindSubscription(SubscriptionName.of("orders"));
    SagaRunner<State> byBuilder = runner(b -> b.parallelism(2));
    SagaRunner<State> byGlobal = runner(b -> b);
    SagaRunner<State> byDefault = runner(b -> b);
    SagaParallelismSettings settings = new SagaParallelismSettings(8, Map.of("orders", 16));
    SagaParallelismDefault.apply(List.of(bySubscription, byBuilder, byGlobal), settings);
    byGlobal.bindSubscription(SubscriptionName.of("x"));
    byGlobal.bindSubscription(SubscriptionName.of("y"));

    assertThat(
            SagaParallelismDefault.startupLines(
                List.of(bySubscription, byBuilder, byGlobal, byDefault), 2))
        .containsExactly(
            "StreamRune: saga type "
                + TYPE
                + " (subscription 'orders'): parallelism 16"
                + " (streamrune.saga.subscriptions.orders.parallelism); its steps hold up to 32"
                + " connections at once",
            "StreamRune: saga type "
                + TYPE
                + " (subscription none bound): parallelism 2 (SagaRunner.Builder.parallelism);"
                + " its steps hold up to 4 connections at once",
            "StreamRune: saga type "
                + TYPE
                + " (subscription 'x', 'y'): parallelism 8 (streamrune.saga.parallelism) x 2"
                + " subscriptions; its steps hold up to 32 connections at once",
            "StreamRune: saga type "
                + TYPE
                + " (subscription none bound): parallelism 4 (default); its steps hold up to 8"
                + " connections at once");
  }

  @Test
  void stepsInFlightAtMostSumsTheRunners() {
    SagaRunner<State> a = runner(b -> b.parallelism(3));
    SagaRunner<State> b2 = runner(b -> b);

    assertThat(SagaParallelismDefault.stepsInFlightAtMost(List.of(a, b2))).isEqualTo(7);
    assertThat(SagaParallelismDefault.stepsInFlightAtMost(List.of())).isZero();
  }

  @Test
  void theStartupLineNamesTheSubscriptionWhosePropertyWon_notTheFirstBoundName() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("a"));
    runner.bindSubscription(SubscriptionName.of("b"));
    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("b", 6)));

    assertThat(SagaParallelismDefault.startupLines(List.of(runner), 1))
        .containsExactly(
            "StreamRune: saga type "
                + TYPE
                + " (subscription 'a', 'b'): parallelism 6"
                + " (streamrune.saga.subscriptions.b.parallelism) x 2 subscriptions; its steps"
                + " hold up to 12 connections at once");
  }

  @Test
  void aRunnerBoundToKSubscriptionsCanHaveKPagesInFlight_soTheBudgetMultipliesByK() {
    SagaRunner<State> two = runner(b -> b.parallelism(3));
    two.bindSubscription(SubscriptionName.of("a"));
    two.bindSubscription(SubscriptionName.of("b"));
    SagaRunner<State> none = runner(b -> b.parallelism(5));

    assertThat(SagaParallelismDefault.stepsInFlightAtMost(List.of(two, none))).isEqualTo(6 + 5);
    assertThat(SagaParallelismDefault.startupLines(List.of(two), 2))
        .containsExactly(
            "StreamRune: saga type "
                + TYPE
                + " (subscription 'a', 'b'): parallelism 3 (SagaRunner.Builder.parallelism) x 2"
                + " subscriptions; its steps hold up to 12 connections at once");
  }

  @Test
  void aConflictFoundByTheSettingsLeavesTheRunnerUnchanged() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("a"));
    runner.bindSubscription(SubscriptionName.of("b"));

    assertThatThrownBy(
            () ->
                runner.applyParallelismSettings(
                    new SagaParallelismSettings(4, Map.of("a", 2, "b", 3))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Saga type "
                + TYPE
                + " is bound to the subscriptions 'a' and 'b', which set different parallelism"
                + " (2 and 3): set one value");
    assertThat(runner.boundSubscriptionNames())
        .containsExactly(SubscriptionName.of("a"), SubscriptionName.of("b"));
    assertThat(runner.parallelism()).isEqualTo(4);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.DEFAULT);

    // the refused settings were not kept: a third name binds without a conflict, at the default
    runner.bindSubscription(SubscriptionName.of("c"));
    assertThat(runner.boundSubscriptionNames())
        .containsExactly(
            SubscriptionName.of("a"), SubscriptionName.of("b"), SubscriptionName.of("c"));
    assertThat(runner.parallelism()).isEqualTo(4);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.DEFAULT);

    runner.applyParallelismSettings(new SagaParallelismSettings(8, Map.of()));
    assertThat(runner.parallelism()).isEqualTo(8);
    assertThat(runner.parallelismSource()).isEqualTo(SagaParallelismSource.GLOBAL_PROPERTY);
  }

  @Test
  void aRunnerNeverBoundNorGivenSettingsReportsTheBuilderSource() {
    SagaRunner<State> runner = runner(b -> b.parallelism(2));

    assertThat(SagaParallelismDefault.sourceOf(runner)).isEqualTo(SagaParallelismSource.BUILDER);
    assertThat(SagaParallelismDefault.effectiveOf(runner)).isEqualTo(2);
  }

  @Test
  void aKnownNameDifferingOnlyInCaseOrDotFromTheConfiguredKeyFailsStartup() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("Orders.v2"));
    SagaParallelismSettings settings = new SagaParallelismSettings(4, Map.of("orders.v2", 3));

    assertThatThrownBy(
            () -> SagaParallelismDefault.requireKnownSubscriptions(List.of(runner), settings))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "bound to: [orders.v2]. Subscriptions bound to a saga runner:" + " [Orders.v2].");
  }

  @Test
  void aConfiguredKeyIsSanitisedWhereverItIsWrittenIntoAMessage() {
    assertThatThrownBy(() -> new SagaParallelismSettings(4, Map.of("a\nINFO forged", 0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "streamrune.saga.subscriptions.aINFO forged.parallelism must be between 1 and 64,"
                + " got: 0");

    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("known"));
    SagaParallelismSettings settings = new SagaParallelismSettings(4, Map.of("x\ny", 2));
    assertThatThrownBy(
            () -> SagaParallelismDefault.requireKnownSubscriptions(List.of(runner), settings))
        .hasMessageContaining("bound to: [xy].")
        .message()
        .doesNotContain("\n");

    SagaRunner<State> bound = runner(b -> b);
    bound.bindSubscription(SubscriptionName.of("p\nq"));
    assertThat(SagaParallelismDefault.startupLines(List.of(bound), 1).get(0))
        .contains("(subscription 'pq')")
        .doesNotContain("\n");
  }

  @Test
  void theNamesOfAConflictAreSanitised() {
    SagaRunner<State> runner = runner(b -> b);
    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("p\nq", 2, "r\ns", 3)));
    runner.bindSubscription(SubscriptionName.of("p\nq"));

    assertThatThrownBy(() -> runner.bindSubscription(SubscriptionName.of("r\ns")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("subscriptions 'pq' and 'rs'")
        .message()
        .doesNotContain("\n");
  }

  @Test
  void theWinningNameInTheLabelAndTheKnownNamesAreSanitised() {
    SagaRunner<State> runner = runner(b -> b);
    runner.bindSubscription(SubscriptionName.of("w\nx"));
    runner.applyParallelismSettings(new SagaParallelismSettings(4, Map.of("w\nx", 6)));

    assertThat(SagaParallelismDefault.startupLines(List.of(runner), 1).get(0))
        .contains("(streamrune.saga.subscriptions.wx.parallelism)")
        .doesNotContain("\n");
    SagaParallelismSettings unknown = new SagaParallelismSettings(4, Map.of("zz", 2));
    assertThatThrownBy(
            () -> SagaParallelismDefault.requireKnownSubscriptions(List.of(runner), unknown))
        .hasMessageContaining("bound to a saga runner: [wx].")
        .message()
        .doesNotContain("\n");
  }

  private static SagaRunner<State> runner(UnaryOperator<SagaRunner.Builder<State>> setting) {
    InMemorySagaStore rows = new InMemorySagaStore();
    InMemoryEventStore eventStore =
        new InMemoryEventStore(TYPES)
            .withCommandInbox(new InMemoryCommandInbox())
            .withSagaStore(rows);
    VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(new InMemoryCommandInbox())
            .build();
    return setting
        .apply(
            SagaRunner.<State>builder()
                .decider(new SagaStepLockFixture.OrderSaga())
                .startWhen(e -> e.event() instanceof Opened)
                .extractSagaId(e -> SagaStepLockFixture.sagaOf(((Opened) e.event()).orderId()))
                .correlateByEventData((streamId, e) -> Optional.empty())
                .sagaStore(rows)
                .commandBus(bus)
                .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(rows)))
        .build();
  }
}
