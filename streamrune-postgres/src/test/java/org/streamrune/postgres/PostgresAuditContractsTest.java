package org.streamrune.postgres;

import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.test.AuditStoreContract;
import org.streamrune.test.CommandAuditQueryContract;
import org.streamrune.test.ComplianceReportQueryContract;
import org.streamrune.test.EventAuditQueryContract;
import org.streamrune.test.EventAuditStoreContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The shared audit contracts against the real SQL on the shipped baseline schema: the command and
 * event audit stores and the three queries over them. There is no in-memory audit store, so these
 * are the only runs of the contracts until the SQLite stores join.
 */
@Testcontainers
class PostgresAuditContractsTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("audit_contract");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  /** Empties both audit tables; every contract case starts from an empty trail. */
  static DataSource cleanDataSource() {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM audit_log");
      stmt.execute("DELETE FROM event_audit_log");
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return dataSource;
  }

  @Nested
  class AuditStoreContractTest extends AuditStoreContract {
    @Override
    protected AuditStore newStore() {
      return new PostgresAuditStore(cleanDataSource());
    }
  }

  @Nested
  class EventAuditStoreContractTest extends EventAuditStoreContract {
    @Override
    protected EventAuditStore newStore() {
      return new PostgresEventAuditStore(cleanDataSource());
    }
  }

  @Nested
  class CommandAuditQueryContractTest extends CommandAuditQueryContract {
    @Override
    protected AuditStore writer() {
      return new PostgresAuditStore(cleanDataSource());
    }

    @Override
    protected CommandAuditQuery query() {
      return new PostgresCommandAuditQuery(dataSource);
    }
  }

  @Nested
  class EventAuditQueryContractTest extends EventAuditQueryContract {
    @Override
    protected EventAuditStore writer() {
      return new PostgresEventAuditStore(cleanDataSource());
    }

    @Override
    protected EventAuditQuery query() {
      return new PostgresEventAuditQuery(dataSource);
    }
  }

  @Nested
  class ComplianceReportQueryContractTest extends ComplianceReportQueryContract {
    @Override
    protected AuditStore writer() {
      return new PostgresAuditStore(cleanDataSource());
    }

    @Override
    protected ComplianceReportQuery query() {
      return new PostgresComplianceReportQuery(dataSource);
    }
  }
}
