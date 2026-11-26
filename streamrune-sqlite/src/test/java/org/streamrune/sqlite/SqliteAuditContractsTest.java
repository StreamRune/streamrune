package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;
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
import org.streamrune.test.EventStoreContract;

/**
 * The shared audit contracts against the SQLite stores and queries, each case on a fresh database
 * file migrated by {@link SqliteEventStoreFactory}: the command and event audit stores, the three
 * queries over them, and the two stores once more on a HikariCP pool of one connection, where the
 * two writers of a concurrent case queue for the one connection.
 */
class SqliteAuditContractsTest {

  @TempDir static Path dir;

  private static final AtomicInteger FILES = new AtomicInteger();

  private final List<AutoCloseable> resources = new ArrayList<>();

  /** The database of the current case: {@code writer()} opens it, {@code query()} reads it. */
  private SqliteDatabase database;

  @AfterEach
  void close() throws Exception {
    for (int i = resources.size() - 1; i >= 0; i--) {
      resources.get(i).close();
    }
  }

  /** A new, empty, migrated database file: an empty trail. */
  private SqliteDatabase fresh() {
    return migrated(TestDatabases.dataSource(dir.resolve("audit-" + FILES.incrementAndGet())));
  }

  /** A new, empty, migrated database file behind a HikariCP pool of one connection. */
  private SqliteDatabase freshOnAPoolOfOne() {
    HikariDataSource pool =
        TestDatabases.poolOfOne(
            dir.resolve("pooled-audit-" + FILES.incrementAndGet()), "audit-contract-pool");
    resources.add(pool);
    return migrated(pool);
  }

  private SqliteDatabase migrated(DataSource dataSource) {
    database = SqliteDatabase.open(dataSource);
    resources.add(database);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  @Nested
  class AuditStoreContractTest extends AuditStoreContract {
    @Override
    protected AuditStore newStore() {
      return new SqliteAuditStore(fresh());
    }
  }

  @Nested
  class AuditStoreContractOnAPoolOfOneTest extends AuditStoreContract {
    @Override
    protected AuditStore newStore() {
      return new SqliteAuditStore(freshOnAPoolOfOne());
    }
  }

  @Nested
  class EventAuditStoreContractTest extends EventAuditStoreContract {
    @Override
    protected EventAuditStore newStore() {
      return new SqliteEventAuditStore(fresh());
    }
  }

  @Nested
  class EventAuditStoreContractOnAPoolOfOneTest extends EventAuditStoreContract {
    @Override
    protected EventAuditStore newStore() {
      return new SqliteEventAuditStore(freshOnAPoolOfOne());
    }
  }

  @Nested
  class CommandAuditQueryContractTest extends CommandAuditQueryContract {
    @Override
    protected AuditStore writer() {
      return new SqliteAuditStore(fresh());
    }

    @Override
    protected CommandAuditQuery query() {
      return new SqliteCommandAuditQuery(database);
    }
  }

  @Nested
  class EventAuditQueryContractTest extends EventAuditQueryContract {
    @Override
    protected EventAuditStore writer() {
      return new SqliteEventAuditStore(fresh());
    }

    @Override
    protected EventAuditQuery query() {
      return new SqliteEventAuditQuery(database);
    }
  }

  @Nested
  class EventAuditQueryContractOnAPoolOfOneTest extends EventAuditQueryContract {
    @Override
    protected EventAuditStore writer() {
      return new SqliteEventAuditStore(freshOnAPoolOfOne());
    }

    @Override
    protected EventAuditQuery query() {
      return new SqliteEventAuditQuery(database);
    }
  }

  @Nested
  class ComplianceReportQueryContractTest extends ComplianceReportQueryContract {
    @Override
    protected AuditStore writer() {
      return new SqliteAuditStore(fresh());
    }

    @Override
    protected ComplianceReportQuery query() {
      return new SqliteComplianceReportQuery(database);
    }
  }
}
