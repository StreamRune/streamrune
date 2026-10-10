package org.streamrune.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.resource.LoadableResource;

/**
 * A Flyway migration series the framework ships, handed to Flyway as an explicit list of scripts
 * rather than a classpath location for Flyway to list.
 *
 * <p><b>Why Flyway does not scan.</b> Flyway lists a {@code classpath:} location through the URL
 * the class loader returns for the directory, and understands only the {@code file} and {@code jar}
 * protocols (plus a few application-server ones). A GraalVM native image serves its resources under
 * the {@code resource} protocol: Flyway logs "unsupported protocol" and applies nothing, and the
 * schema validation that follows refuses every framework table as missing, so the default
 * auto-initialization could not work in a native image. Each script is instead looked up by its
 * exact name with {@link ClassLoader#getResource(String)}, which a native image answers for every
 * resource registered at build time; the Spring, Quarkus and Micronaut integrations register both
 * series. A script that cannot be found is refused, naming the script and the registration it
 * needs, before Flyway touches the database.
 *
 * <p>The scripts resolve to the same resources a scan would find on the JVM: same file name, same
 * relative path (the {@code script} column of the history table) and same content, hence the same
 * checksum.
 *
 * <p><b>Adding a migration.</b> A new script must be added to its series directory AND to the list
 * here; a test compares each list with what Flyway's own classpath scan finds on the JVM.
 */
final class ShippedMigrationSeries {

  /** The event-store series, shipped in {@code streamrune-postgres}. */
  static final ShippedMigrationSeries EVENT_STORE =
      new ShippedMigrationSeries(
          PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION,
          "streamrune-postgres",
          List.of("V001__streamrune_baseline.sql"));

  /** The crypto series, shipped in {@code streamrune-postgres-crypto}. */
  static final ShippedMigrationSeries CRYPTO =
      new ShippedMigrationSeries(
          "classpath:db/crypto-migration",
          "streamrune-postgres-crypto",
          List.of("V001__crypto_baseline.sql"));

  private static final String CLASSPATH_PREFIX = "classpath:";

  /**
   * The logging back end Flyway is told to use: SLF4J, the facade the framework itself logs
   * through.
   *
   * <p>Left to its default ({@code auto}), Flyway picks a back end by probing for SLF4J, Log4j 2
   * and Apache Commons Logging by class name, and instantiates the matching log creator
   * reflectively. In a GraalVM native image the probe sees only the classes registered for
   * reflection, so the outcome depends on what the rest of the image happens to register, and a log
   * creator chosen that way but not registered itself stops Flyway with "Unable to instantiate
   * class". Naming the back end leaves one log creator to register, which the Quarkus and Micronaut
   * integrations do.
   */
  static final String FLYWAY_LOGGER = "slf4j";

  private final String location;
  private final String directory;
  private final String artifact;
  private final List<String> scripts;

  private ShippedMigrationSeries(String location, String artifact, List<String> scripts) {
    this.location = location;
    this.directory = location.substring(CLASSPATH_PREFIX.length());
    this.artifact = artifact;
    this.scripts = List.copyOf(scripts);
  }

  /** The Flyway location of this series, e.g. {@code classpath:db/streamrune-migration}. */
  String location() {
    return location;
  }

  /** The class-path resource name of every script of this series, in version order. */
  List<String> scriptPaths() {
    return scripts.stream().map(script -> directory + "/" + script).toList();
  }

  /**
   * A Flyway configuration that applies this series to {@code dataSource}. The caller sets the
   * history table and the baseline.
   *
   * <p>Every script is read here, through the class loader Flyway itself resolves migrations with
   * (the thread context class loader), so a missing script is refused before anything is written.
   *
   * @throws IllegalStateException if a script of this series is not on the class path
   */
  FluentConfiguration configure(DataSource dataSource) {
    FluentConfiguration configuration = Flyway.configure();
    ClassLoader classLoader = configuration.getClassLoader();
    if (classLoader == null) {
      classLoader = ShippedMigrationSeries.class.getClassLoader();
    }
    List<LoadableResource> resources = read(classLoader, configuration.getEncoding());
    return configuration
        .dataSource(dataSource)
        .locations(location)
        .loggers(FLYWAY_LOGGER)
        .resourceProvider(new Scripts(resources))
        .javaMigrationClassProvider(List::<Class<? extends JavaMigration>>of);
  }

  private List<LoadableResource> read(ClassLoader classLoader, Charset encoding) {
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
                + "/*.sql as a resource (the Spring, Quarkus and Micronaut integrations do);"
                + " otherwise check that "
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
   * classpath scan finds directly in the location directory.
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
      return new StringReader(content);
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
