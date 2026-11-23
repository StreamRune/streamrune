package org.streamrune.sqlite;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.resource.LoadableResource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.LogSanitizer;

/**
 * Applies a schema series to a SQLite database with Flyway, from an explicit list of scripts.
 *
 * <p><b>No classpath scanning.</b> Each script is looked up by its exact name with {@link
 * ClassLoader#getResource(String)}, which a GraalVM native image answers for every registered
 * resource, and handed to Flyway through a {@link ResourceProvider}; Flyway never lists a
 * directory. A script that cannot be found is refused, naming the script and the registration it
 * needs, before Flyway touches the database.
 *
 * <p><b>Baseline 0.</b> Every series runs on its own history table and is baselined at version
 * {@code 0}, as on Postgres: it applies in full, and tolerates a database that already holds the
 * application's tables or another series'.
 *
 * <p><b>Crash points.</b> Flyway runs each SQLite script in one transaction together with the
 * history row that records it, because SQLite has transactional DDL. A crash, or a failure, in the
 * middle of a script therefore leaves neither the script's tables nor its history row, and the next
 * run applies the script again from its start. Creating the history table (with the baseline row,
 * on a non-empty database) is one transaction of its own; a crash after it leaves a history table
 * without migrations, on which the next run applies every script. Each series has its own history,
 * so a crash between two series leaves the first one complete and the second one not started, and
 * the next run applies only what is missing.
 *
 * <p><b>No first-start race.</b> On Postgres, replicas that start together can race through
 * Flyway's creation of the history table, which {@code SeriesMigration} converges by running once
 * more. On SQLite the instance guard admits one process per database file, and in this process
 * every migration runs under one lock, so no second migration can create the history table while
 * another one is deciding whether to create it. Flyway's own SQLite lock is a no-op, so the lock
 * here is what serializes two handles of one file.
 */
final class SqliteSchemaMigration {

  /**
   * The logging back end Flyway is told to use: SLF4J. Left to its default, Flyway probes for
   * logging libraries by class name, which in a native image depends on what the image registers.
   */
  static final String FLYWAY_LOGGER = "slf4j";

  /** Serializes every migration of this process. Migrations run once at startup. */
  private static final ReentrantLock MIGRATIONS = new ReentrantLock();

  private SqliteSchemaMigration() {}

  /** Applies {@code series} to {@code dataSource}. */
  static void migrate(SqliteSchemaSeries series, DataSource dataSource) {
    migrate(
        dataSource, series.directory(), series.historyTable(), series.artifact(), series.scripts());
  }

  /**
   * Applies the scripts of {@code directory} to {@code dataSource}, recorded in {@code
   * historyTable}.
   *
   * @throws IllegalStateException if a script is not on the class path; nothing is written then
   * @throws EventStoreException if Flyway fails; the message names the history table
   */
  static void migrate(
      DataSource dataSource,
      String directory,
      String historyTable,
      String artifact,
      List<String> scripts) {
    Flyway flyway =
        configure(dataSource, directory, artifact, scripts)
            .table(historyTable)
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load();
    MIGRATIONS.lock();
    try {
      flyway.migrate();
    } catch (FlywayException e) {
      throw new EventStoreException(
          "Migrating the SQLite schema series "
              + directory
              + " (history table "
              + historyTable
              + ") failed: "
              + LogSanitizer.sanitizeFreeText(e.getMessage()),
          e);
    } finally {
      MIGRATIONS.unlock();
    }
  }

  /**
   * A Flyway configuration that applies the listed scripts of {@code directory} to {@code
   * dataSource}. Every script is read here, through the class loader Flyway resolves migrations
   * with, so a missing script is refused before anything is written.
   *
   * @throws IllegalStateException if a script is not on the class path
   */
  static FluentConfiguration configure(
      DataSource dataSource, String directory, String artifact, List<String> scripts) {
    FluentConfiguration configuration = Flyway.configure();
    ClassLoader classLoader = configuration.getClassLoader();
    if (classLoader == null) {
      classLoader = SqliteSchemaMigration.class.getClassLoader();
    }
    List<LoadableResource> resources =
        read(classLoader, configuration.getEncoding(), directory, artifact, scripts);
    return configuration
        .dataSource(dataSource)
        .locations("classpath:" + directory)
        .loggers(FLYWAY_LOGGER)
        .resourceProvider(new Scripts(resources))
        .javaMigrationClassProvider(List::<Class<? extends JavaMigration>>of);
  }

  private static List<LoadableResource> read(
      ClassLoader classLoader,
      Charset encoding,
      String directory,
      String artifact,
      List<String> scripts) {
    List<LoadableResource> resources = new ArrayList<>(scripts.size());
    for (String script : scripts) {
      String path = directory + "/" + script;
      URL url = classLoader.getResource(path);
      if (url == null) {
        throw new IllegalStateException(
            "StreamRune migration script "
                + path
                + " is not on the class path, so the schema cannot be initialized. In a GraalVM"
                + " native image register "
                + directory
                + "/*.sql as a resource; otherwise check that "
                + artifact
                + " is on the class path, or provision the schema yourself and turn schema"
                + " auto-initialization off.");
      }
      String content;
      try (InputStream in = url.openStream()) {
        content = new String(in.readAllBytes(), encoding);
      } catch (IOException e) {
        throw new IllegalStateException(
            "StreamRune migration script " + path + " could not be read: " + e.getMessage(), e);
      }
      resources.add(new Script(script, path, url.toExternalForm(), content));
    }
    return List.copyOf(resources);
  }

  /** Answers Flyway's resource queries from the scripts read up front. */
  private record Scripts(List<LoadableResource> resources) implements ResourceProvider {

    @Override
    public LoadableResource getResource(String name) {
      for (LoadableResource resource : resources) {
        if (resource.getRelativePath().equals(name)) {
          return resource;
        }
      }
      return null;
    }

    @Override
    public Collection<LoadableResource> getResources(String prefix, String[] suffixes) {
      List<LoadableResource> matching = new ArrayList<>();
      for (LoadableResource resource : resources) {
        String fileName = resource.getFilename();
        if (fileName.startsWith(prefix) && endsWithAny(fileName, suffixes)) {
          matching.add(resource);
        }
      }
      return matching;
    }

    private static boolean endsWithAny(String fileName, String[] suffixes) {
      for (String suffix : suffixes) {
        if (fileName.endsWith(suffix)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * One script, held in memory. Its relative path is its file name, as for a resource Flyway's
   * classpath scan finds directly in the location directory, so the history table records the same
   * script name and checksum a scan would.
   */
  private static final class Script extends LoadableResource {

    private final String fileName;
    private final String path;
    private final String url;
    private final String content;

    Script(String fileName, String path, String url, String content) {
      this.fileName = fileName;
      this.path = path;
      this.url = url;
      this.content = content;
    }

    @Override
    public Reader read() {
      return Reader.of(content);
    }

    @Override
    public String getAbsolutePath() {
      return path;
    }

    @Override
    public String getAbsolutePathOnDisk() {
      return url;
    }

    @Override
    public String getFilename() {
      return fileName;
    }

    @Override
    public String getRelativePath() {
      return getFilename();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Script script && path.equals(script.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }
  }
}
