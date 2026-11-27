package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.EnvConfigSource;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.LogSanitizer;

/**
 * Refuses the start when a key under {@code streamrune.projections.} is not one the projection
 * discovery reads: {@code auto-discovery.enabled}, or {@code <name>.enabled} or {@code
 * <name>.error-strategy} where {@code <name>} is the name of a {@link ProjectionConfig} projection
 * bean (a quoted name, {@code "orders.v2".enabled}, names {@code orders.v2}).
 *
 * <p>The configuration mapping declares every key under {@code streamrune.projections.} as one map
 * ({@link StreamRuneQuarkusProperties#projections()}), so that SmallRye accepts the keys the
 * discovery reads by name; this check refuses the others — a misspelt {@code enabled}, a name no
 * projection has — as SmallRye refuses any other {@code streamrune.} key no member of the mapping
 * declares, and like SmallRye it is off when {@code quarkus.config.mapping.validate-unknown} (or
 * {@code smallrye.config.mapping.validate-unknown}) is {@code false}. A name is checked against the
 * projection beans of the running application, so a key for a projection that exists in one profile
 * only belongs under that profile ({@code %prod.streamrune.projections.<name>.enabled}), and no
 * name is checked while {@code streamrune.projections.auto-discovery.enabled} is {@code false}: the
 * discovery then reads no {@code <name>} key. A key whose value comes from the process environment
 * or a {@code .env} file is not checked, as SmallRye checks no such key: the variable {@code
 * STREAMRUNE_PROJECTIONS_ORDER_TOTALS_ERROR_STRATEGY} is the map key {@code
 * order.totals.error.strategy}, which cannot be told apart from a misspelling. It runs before the
 * runners start.
 */
@ApplicationScoped
public class ProjectionKeysValidator {

  private static final String PREFIX = "streamrune.projections.";

  private static final String AUTO_DISCOVERY = "auto-discovery.enabled";

  private static final List<String> SUFFIXES = List.of(".enabled", ".error-strategy");

  /** The switches of SmallRye's own refusal of an unknown key, as Quarkus and SmallRye name it. */
  private static final List<String> VALIDATE_UNKNOWN =
      List.of(
          "quarkus.config.mapping.validate-unknown", "smallrye.config.mapping.validate-unknown");

  private final StreamRuneQuarkusProperties properties;
  private final Instance<Config> config;
  private final Instance<Projection> projections;

  @jakarta.inject.Inject
  public ProjectionKeysValidator(
      StreamRuneQuarkusProperties properties,
      Instance<Config> config,
      Instance<Projection> projections) {
    this.properties = properties;
    this.config = config;
    this.projections = projections;
  }

  /**
   * Client-proxy constructor, as in {@link SagaEventPathRetentionConfigurer}: never used for a real
   * instance.
   */
  protected ProjectionKeysValidator() {
    this.properties = null;
    this.config = null;
    this.projections = null;
  }

  /**
   * Startup observer, before the runners start. {@code public} for the observer invoker Arc
   * generates, see {@link SagaEventPathRetentionConfigurer}.
   *
   * @param event the startup event
   * @throws IllegalStateException when a key under {@code streamrune.projections.} is not read
   */
  public void validate(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    Config applicationConfig = SagaParallelismProperties.applicationConfig(config);
    if (properties.projections().isEmpty() || !validatesUnknownKeys(applicationConfig)) {
      return;
    }
    check(
        properties.projections().keySet(),
        applicationConfig,
        // The discovery reads no <name> key while it is off: a name is then not checked.
        ProjectionProducer.autoDiscoveryEnabled(applicationConfig) ? this::projectionNames : null);
  }

  /**
   * Refuses the keys, written after {@code streamrune.projections.}, that the discovery does not
   * read.
   *
   * @param keys the keys after the prefix
   * @param config the configuration the keys come from
   * @param projectionNames the names of the {@link ProjectionConfig} projections, looked up only
   *     when a {@code <name>} key is to be checked; {@code null} checks no name
   * @throws IllegalStateException naming every refused key, when there is one
   */
  static void check(Collection<String> keys, Config config, Supplier<Set<String>> projectionNames) {
    List<String> notRead = new ArrayList<>();
    Map<String, String> named = new TreeMap<>();
    for (String key : new TreeSet<>(keys)) {
      if (AUTO_DISCOVERY.equals(key) || fromTheEnvironment(config, key)) {
        continue;
      }
      String name = nameOf(key);
      if (name == null) {
        notRead.add(PREFIX + LogSanitizer.sanitizeForLog(key));
      } else {
        named.put(key, name);
      }
    }
    Set<String> names =
        named.isEmpty() || projectionNames == null ? Set.of() : projectionNames.get();
    List<String> unknownName = new ArrayList<>();
    if (projectionNames != null) {
      named.forEach(
          (key, name) -> {
            if (!names.contains(name)) {
              unknownName.add(PREFIX + LogSanitizer.sanitizeForLog(key));
            }
          });
    }
    List<String> problems = new ArrayList<>();
    if (!notRead.isEmpty()) {
      problems.add(
          String.join(", ", notRead)
              + (notRead.size() == 1 ? " is not a key" : " are not keys")
              + " the projection discovery reads: under "
              + PREFIX
              + " it reads auto-discovery.enabled, <name>.enabled and <name>.error-strategy");
    }
    if (!unknownName.isEmpty()) {
      problems.add(
          String.join(", ", unknownName)
              + (unknownName.size() == 1 ? " names" : " name")
              + " no @ProjectionConfig projection; the projections are "
              + (names.isEmpty()
                  ? "none"
                  : String.join(
                      ", ",
                      new TreeSet<>(names.stream().map(LogSanitizer::sanitizeForLog).toList()))));
    }
    if (!problems.isEmpty()) {
      throw new IllegalStateException(String.join(". ", problems) + ".");
    }
  }

  /**
   * The projection name of {@code <name>.enabled} or {@code <name>.error-strategy}, without the
   * double quotes of a quoted name; {@code null} for any other key.
   */
  private static String nameOf(String key) {
    for (String suffix : SUFFIXES) {
      if (key.endsWith(suffix) && key.length() > suffix.length()) {
        String name = key.substring(0, key.length() - suffix.length());
        return name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")
            ? name.substring(1, name.length() - 1)
            : name;
      }
    }
    return null;
  }

  /** The source SmallRye exempts from its own refusal: the environment, a {@code .env} file. */
  private static boolean fromTheEnvironment(Config config, String key) {
    ConfigValue value = config.getConfigValue(PREFIX + key);
    String source = value.getSourceName();
    return source != null && source.startsWith(EnvConfigSource.NAME);
  }

  /**
   * Whether SmallRye refuses an unknown key: read as SmallRye reads its switch, with the boolean
   * converter of the configuration, so {@code off} or {@code no} turn it off as {@code false} does.
   * Quarkus relocates either name to the other; both are read for a configuration without that
   * relocation.
   */
  private static boolean validatesUnknownKeys(Config config) {
    for (String key : VALIDATE_UNKNOWN) {
      if (!config.getOptionalValue(key, Boolean.class).orElse(true)) {
        return false;
      }
    }
    return true;
  }

  /** The names of the {@link ProjectionConfig} projection beans, as the discovery finds them. */
  private Set<String> projectionNames() {
    Set<String> names = new TreeSet<>();
    for (Projection projection : projections) {
      ProjectionConfig projectionConfig =
          ProjectionProducer.findProjectionConfig(projection.getClass());
      if (projectionConfig != null) {
        names.add(projectionConfig.name());
      }
    }
    return names;
  }
}
