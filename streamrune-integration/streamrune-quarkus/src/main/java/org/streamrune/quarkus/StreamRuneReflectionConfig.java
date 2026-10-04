package org.streamrune.quarkus;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.ValidationError;
import org.streamrune.core.Versioned;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.gdpr.GdprAction;
import org.streamrune.core.gdpr.SubjectExport;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorClass;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.Window;
import org.streamrune.core.projection.WindowResult;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * Registers StreamRune core types for GraalVM native image reflection.
 *
 * <p>All value types and core records need constructor and field access for JSON serialisation and
 * ORM mapping when compiled to a native image.
 *
 * <p><b>Projections.</b> {@link Projection#writesThroughRepository()} resolves {@code process(List,
 * ProjectionRepository)} with {@code getClass().getMethod(...)}, which a native image answers only
 * from types registered for reflection; the method is declared by {@link Projection} (the default)
 * or {@link BaseProjection} (final), so both are registered here. An application projection that
 * declares {@code process(List, ProjectionRepository)} itself must register its own class too: the
 * image sees a method declaration only on a type registered for reflection.
 *
 * <p><b>HikariCP.</b> The framework builds small HikariCP pools of its own for its dedicated
 * connections (the notification subscription's and the crypto forget signal's LISTEN connections,
 * and the advisory locker's opt-in dedicated pool); the event store itself borrows from the
 * application's Agroal DataSource. {@code new HikariDataSource(config)} copies the configuration by
 * iterating {@code HikariConfig.class.getDeclaredFields()}, and the pool instantiates arrays and a
 * {@code Connection} proxy reflectively. Spring Boot ships HikariCP hints; Quarkus pools with
 * Agroal and does not, so this module ships them as {@code
 * META-INF/native-image/org.streamrune/streamrune-quarkus/reachability-metadata.json} (each entry
 * conditional on the HikariCP type that needs it). Without them the pool copies an empty
 * configuration and the application refuses to start ({@code Failed to initialize pool: null}).
 */
@RegisterForReflection(
    targets = {
      // Value types — every @JsonValue record in org.streamrune.core.types (18)
      AggregateId.class,
      AggregateType.class,
      CausationId.class,
      CommandId.class,
      CorrelationId.class,
      EventId.class,
      EventType.class,
      GlobalOffset.class,
      IdempotencyKey.class,
      ProjectionName.class,
      SagaType.class,
      SpanId.class,
      StreamId.class,
      SubjectId.class,
      SubscriptionName.class,
      TraceId.class,
      UserId.class,
      Version.class,
      // Core records — org.streamrune.core (11)
      AggregateHistory.class,
      DeadLetterRetryPolicy.class,
      EventEnvelope.class,
      EventMetadata.class,
      Page.class,
      PageRequest.class,
      RetryPolicy.class,
      Sort.class,
      UserAuthority.class,
      ValidationError.class,
      Versioned.class,
      // Core nested record + enum — org.streamrune.core (2)
      Sort.Order.class,
      SortDirection.class,
      // Core records — org.streamrune.core.audit (3)
      AuditEntry.class,
      ComplianceReport.class,
      EventAuditEntry.class,
      // Core enum — org.streamrune.core.audit (1)
      AuditOutcome.class,
      // Core records — org.streamrune.core.gdpr (1)
      SubjectExport.class,
      // Core records — org.streamrune.core.outbox (3)
      OutboxEntry.class,
      OutboxEntry.SkipRecord.class,
      OutboxEntryId.class,
      // Core enums — org.streamrune.core.outbox (2)
      OutboxStatus.class,
      OutboxOrderingMode.class,
      // Core records — org.streamrune.core.projection (3)
      ProjectionDeadLetterEntry.class,
      Window.class,
      WindowResult.class,
      // Core records — org.streamrune.core.saga (2)
      SagaCommand.class,
      SagaId.class,
      // Core records — org.streamrune.core.subscription (2)
      SubscriptionConfig.class,
      SubscriptionHealth.class,
      // Core enums — org.streamrune.core.subscription (2)
      SubscriptionHealth.Status.class,
      SubscriptionLifecycleState.class,
      // Top-level core enums/records the exhaustive scan now demands; the
      // ProjectionDeliveryMode @ProjectionConfig declares joins them (8)
      LockMode.class,
      GdprAction.class,
      LateDataPolicy.class,
      ProjectionErrorClass.class,
      ProjectionErrorStrategy.class,
      ProjectionDeliveryMode.class,
      LoadedSaga.class,
      SagaStatus.class,
      // Projection types whose process(List, ProjectionRepository) is resolved reflectively by
      // Projection#writesThroughRepository (2)
      Projection.class,
      BaseProjection.class,
    })
public final class StreamRuneReflectionConfig {}
