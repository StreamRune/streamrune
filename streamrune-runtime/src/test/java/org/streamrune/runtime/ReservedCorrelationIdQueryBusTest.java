package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.Auditable;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * The query buses refuse what the command buses refuse: a dispatch under a correlation id in the
 * reserved form that is not the bound saga origin's.
 *
 * <p>A query stores no event, but an {@code @Auditable} one stores an audit row with the bound
 * correlation id. An ingress that builds its request context with the plain constructor can bind a
 * client's {@code streamrune:saga:<a saga's id>}; without the refusal such a request would leave an
 * audit row that carries a saga's reserved correlation id although no saga took part. With it, an
 * audit row a shipped bus writes carries a reserved correlation id only for a dispatch that ran
 * with that saga's origin bound.
 */
class ReservedCorrelationIdQueryBusTest {

  @Auditable
  record FindOrder(String id) implements Query<String> {}

  record FindProduct(String id) implements Query<String> {}

  @Cacheable(ttlSeconds = 60)
  record FindCatalogue(String id) implements Query<String> {}

  private static final SagaOrigin ORIGIN = SagaOrigins.of("order-saga-42");

  private static final String REFUSAL =
      "the bound correlation id uses the reserved 'streamrune:' prefix and is not the bound saga"
          + " origin's; only a saga dispatch or StreamRuneContext.continuing binds it";

  /** A bus under test with what it would record: the handler calls and the audit rows. */
  private record Subject(QueryBus bus, AtomicInteger handled, List<AuditEntry> audit) {}

  private static void register(QueryBus bus, AtomicInteger handled) {
    bus.register(FindOrder.class, query -> "order-" + handled.incrementAndGet());
    bus.register(FindProduct.class, query -> "product-" + handled.incrementAndGet());
    bus.register(FindCatalogue.class, query -> "catalogue-" + handled.incrementAndGet());
  }

  /** Every shipped query bus, alone and stacked as the integrations stack them. */
  private static Map<String, Supplier<Subject>> buses() {
    Map<String, Supplier<Subject>> buses = new LinkedHashMap<>();
    buses.put(
        "SimpleQueryBus",
        () -> {
          AtomicInteger handled = new AtomicInteger();
          QueryBus bus = new SimpleQueryBus();
          register(bus, handled);
          return new Subject(bus, handled, new CopyOnWriteArrayList<>());
        });
    buses.put(
        "CachingQueryBus over SimpleQueryBus",
        () -> {
          AtomicInteger handled = new AtomicInteger();
          QueryBus bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
          register(bus, handled);
          return new Subject(bus, handled, new CopyOnWriteArrayList<>());
        });
    buses.put(
        "AuditingQueryBus over SimpleQueryBus",
        () -> {
          AtomicInteger handled = new AtomicInteger();
          List<AuditEntry> audit = new CopyOnWriteArrayList<>();
          QueryBus bus = new AuditingQueryBus(new SimpleQueryBus(), audit::add);
          register(bus, handled);
          return new Subject(bus, handled, audit);
        });
    buses.put(
        "AuditingQueryBus over CachingQueryBus over SimpleQueryBus",
        () -> {
          AtomicInteger handled = new AtomicInteger();
          List<AuditEntry> audit = new CopyOnWriteArrayList<>();
          QueryBus bus =
              new AuditingQueryBus(
                  CachingQueryBus.builder().delegate(new SimpleQueryBus()).build(), audit::add);
          register(bus, handled);
          return new Subject(bus, handled, audit);
        });
    return buses;
  }

  private static StreamRuneContext.RequestContext contextWith(String correlationId) {
    return new StreamRuneContext.RequestContext(
        null, UserId.of("caller-1"), CorrelationId.of(correlationId), Instant.now(), Map.of());
  }

  private static List<Query<String>> queries() {
    return List.of(new FindOrder("o-1"), new FindProduct("p-1"), new FindCatalogue("c-1"));
  }

  @TestFactory
  Stream<DynamicTest> aQueryUnderAForeignReservedCorrelationId_isRefusedBeforeAnythingIsRecorded() {
    Map<String, ScopedValue.Carrier> bindings = new LinkedHashMap<>();
    bindings.put(
        "a saga's reserved id, no origin",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:saga:victim")));
    bindings.put(
        "a reserved id that is no saga's, no origin",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:anything")));
    bindings.put(
        "a saga's reserved id beside a cause without an origin",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:saga:victim"))
            .where(
                StreamRuneContext.CAUSE,
                new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), null)));
    bindings.put(
        "another saga's reserved id beside this saga's origin",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:saga:another-saga"))
            .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, ORIGIN)));

    List<DynamicTest> cases = new ArrayList<>();
    buses()
        .forEach(
            (name, subjects) ->
                bindings.forEach(
                    (bound, carrier) ->
                        cases.add(
                            DynamicTest.dynamicTest(
                                name + ": " + bound,
                                () -> {
                                  Subject subject = subjects.get();
                                  for (Query<String> query : queries()) {
                                    assertThatThrownBy(
                                            () -> carrier.call(() -> subject.bus().dispatch(query)))
                                        .isInstanceOf(ReservedCorrelationIdException.class)
                                        .hasMessage(REFUSAL);
                                  }
                                  assertThat(subject.handled()).hasValue(0);
                                  assertThat(subject.audit()).isEmpty();
                                }))));
    return cases.stream();
  }

  @TestFactory
  Stream<DynamicTest> aQueryUnderAnOrdinaryCorrelationIdOrNone_isDispatched() {
    List<DynamicTest> cases = new ArrayList<>();
    buses()
        .forEach(
            (name, subjects) ->
                cases.add(
                    DynamicTest.dynamicTest(
                        name,
                        () -> {
                          Subject subject = subjects.get();
                          for (Query<String> query : queries()) {
                            assertThat(subject.bus().dispatch(query)).isNotBlank();
                          }
                          for (String ordinary :
                              List.of("client-corr-7", "order-saga-42", "Streamrune:saga:x")) {
                            for (Query<String> query : queries()) {
                              assertThat(
                                      ScopedValue.where(
                                              StreamRuneContext.CURRENT, contextWith(ordinary))
                                          .call(() -> subject.bus().dispatch(query)))
                                  .isNotBlank();
                            }
                          }
                          assertThat(subject.audit())
                              .extracting(
                                  entry ->
                                      entry.correlationId() == null
                                          ? null
                                          : entry.correlationId().value())
                              .doesNotContain("streamrune:saga:order-saga-42")
                              .allSatisfy(
                                  id -> {
                                    if (id != null) {
                                      assertThat(id).doesNotStartWith("streamrune:");
                                    }
                                  });
                        })));
    return cases.stream();
  }

  /**
   * A query dispatched inside a saga's dispatch, or inside a continuation of a saga-caused event (a
   * decider or an interceptor that reads a model), runs with the saga's reserved correlation id and
   * its origin bound and is not refused. Its audit row carries the origin's correlation id.
   */
  @TestFactory
  Stream<DynamicTest> aQueryInsideASagasDispatch_isDispatched_andAuditedUnderTheOriginsId() {
    Map<String, ScopedValue.Carrier> bindings = new LinkedHashMap<>();
    bindings.put(
        "as saga dispatch binds it",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:saga:order-saga-42"))
            .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
            .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, ORIGIN)));
    bindings.put(
        "a context of the application's own beside the origin",
        ScopedValue.where(StreamRuneContext.CURRENT, contextWith("app-own-correlation"))
            .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, ORIGIN)));

    List<DynamicTest> cases = new ArrayList<>();
    buses()
        .forEach(
            (name, subjects) ->
                bindings.forEach(
                    (bound, carrier) ->
                        cases.add(
                            DynamicTest.dynamicTest(
                                name + ": " + bound,
                                () -> {
                                  Subject subject = subjects.get();
                                  for (Query<String> query : queries()) {
                                    assertThat(carrier.call(() -> subject.bus().dispatch(query)))
                                        .isNotBlank();
                                  }
                                  assertThat(subject.handled()).hasValue(3);
                                  assertThat(subject.audit())
                                      .allSatisfy(
                                          entry -> {
                                            assertThat(entry.outcome())
                                                .isEqualTo(AuditOutcome.SUCCESS);
                                            assertThat(entry.correlationId())
                                                .isEqualTo(ORIGIN.correlationId());
                                          });
                                }))));
    return cases.stream();
  }

  @Test
  void theRefusalIsLoggedOnceAtWarn_byTheOutermostBus_withoutTheBoundValue() {
    Subject subject =
        buses().get("AuditingQueryBus over CachingQueryBus over SimpleQueryBus").get();
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    List<ch.qos.logback.classic.Logger> loggers =
        Stream.of(AuditingQueryBus.class, CachingQueryBus.class, SimpleQueryBus.class)
            .map(type -> (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(type))
            .toList();
    loggers.forEach(logger -> logger.addAppender(appender));
    try {
      assertThatThrownBy(
              () ->
                  ScopedValue.where(
                          StreamRuneContext.CURRENT, contextWith("streamrune:saga:victim"))
                      .call(() -> subject.bus().dispatch(new FindOrder("o-1"))))
          .isInstanceOf(ReservedCorrelationIdException.class);
    } finally {
      loggers.forEach(logger -> logger.detachAppender(appender));
      appender.stop();
    }

    assertThat(appender.list)
        .extracting(event -> event.getLevel() + " " + event.getFormattedMessage())
        .containsExactly("WARN Refusing query FindOrder: " + REFUSAL);
  }
}
