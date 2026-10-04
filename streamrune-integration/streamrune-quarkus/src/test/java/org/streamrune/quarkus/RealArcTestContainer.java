package org.streamrune.quarkus;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ArcContainer;
import io.quarkus.arc.processor.BeanArchives;
import io.quarkus.arc.processor.BeanProcessor;
import io.quarkus.arc.processor.ResourceOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationTarget;
import org.jboss.jandex.AnnotationTransformation;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.Indexer;

/**
 * Boots a <strong>real Arc (Quarkus CDI) container</strong> in-process, without the {@code
 * io.quarkus} Gradle plugin or a Maven project layout — both of which the in-process Quarkus test
 * harnesses ({@code @QuarkusTest}, {@code QuarkusUnitTest}) require and neither of which this
 * producers <em>library</em> module has (see the class javadoc of {@link QuarkusBeanWiringTest} for
 * why {@code @QuarkusTest} cannot bootstrap here).
 *
 * <p>This runs the genuine Arc build-time pipeline the Quarkus plugin would run: it indexes the
 * supplied bean classes with Jandex, runs the real {@link BeanProcessor} (real {@code @DefaultBean}
 * resolution, {@code @Produces}/{@code Instance}/{@code @All} injection-point wiring, bean
 * validation), generates the Arc bean bytecode + {@code ComponentsProvider} service, loads it into
 * an isolated {@link URLClassLoader}, and boots a real {@link io.quarkus.arc.impl.ArcContainerImpl}
 * via {@link Arc#initialize()}. Bean lookups then go through real Arc resolution — the exact code
 * path that would have caught the Micronaut DI defect had an equivalent container test existed.
 *
 * <p>Not a general-purpose harness: it indexes exactly the classes handed to it (no classpath
 * scanning), so a test controls the bean set precisely — e.g. include or omit a {@code DataSource}
 * producer to exercise the "framework default present/absent" branches, or add an application
 * producer to prove it wins over the framework's {@code @DefaultBean}.
 */
final class RealArcTestContainer implements AutoCloseable {

  private final URLClassLoader deploymentClassLoader;
  private final ClassLoader previousTccl;
  private final ArcContainer container;

  private RealArcTestContainer(
      URLClassLoader deploymentClassLoader, ClassLoader previousTccl, ArcContainer container) {
    this.deploymentClassLoader = deploymentClassLoader;
    this.previousTccl = previousTccl;
    this.container = container;
  }

  ArcContainer container() {
    return container;
  }

  /**
   * The classloader the generated Arc classes live in — and, for a container booted by {@link
   * #bootIsolatingPackages}, the classes of the isolated packages too. A test looks a bean of an
   * isolated package up by the class this loader defines, not by the one its own classpath sees.
   */
  ClassLoader deploymentClassLoader() {
    return deploymentClassLoader;
  }

  /**
   * Indexes {@code beanClasses}, runs the Arc build-time processor, and boots a real container.
   *
   * @param beanClasses the classes forming the bean archive — producer classes, their bean types,
   *     and any application-defined beans under test. Referenced collaborator types that are not
   *     themselves beans do not need to be listed; Arc resolves injection points against the
   *     bean-defining types present. Every {@code @Produces} method's return type (and each indexed
   *     type's supertype closure) is indexed automatically, so handing this harness a whole
   *     producer class such as {@link StreamRuneProducers} does not require enumerating its
   *     products — Arc computes a bean's types from its declared return type and rejects one that
   *     is missing from the index.
   */
  static RealArcTestContainer boot(List<Class<?>> beanClasses) throws Exception {
    return boot(beanClasses, null);
  }

  /**
   * As {@link #boot(List)}, but with Quarkus's <strong>build-time conditions</strong> evaluated
   * against {@code buildTimeProperties} instead of ignored.
   *
   * <p>{@code @IfBuildProperty} is not an Arc concept: it is resolved one layer up, by {@code
   * io.quarkus.arc.deployment.BuildTimeEnabledProcessor}, which turns a condition that evaluated
   * {@code false} into an annotation transformation — {@code @Vetoed} on a CLASS declaration,
   * {@code @VetoedProducer} on a METHOD or FIELD one — and hands that to the same {@link
   * BeanProcessor} this harness runs. Reproducing that one transformation is what makes a gated
   * deployment observable here at all; {@link #boot(List)} runs the processor with every condition
   * effectively enabled, which is why it could not see a conditional producer whose disposer is not
   * conditional.
   *
   * @param beanClasses as {@link #boot(List)}
   * @param buildTimeProperties the build-time property values (a Quarkus application's {@code
   *     application.properties} at augmentation time); {@code null} disables condition evaluation
   *     entirely, {@code Map.of()} is the default configuration in which no condition holds
   */
  static RealArcTestContainer boot(
      List<Class<?>> beanClasses, Map<String, String> buildTimeProperties) throws Exception {
    return boot(beanClasses, buildTimeProperties, List.of(), Set.of(), false, Set.of());
  }

  /**
   * As {@link #boot(List)}, plus JAX-RS {@code @Provider} classes registered the way Quarkus REST
   * registers them: as CDI beans whose default scope is {@code @Singleton}, constructed by Arc
   * through their {@code @Inject} constructor. {@code @Provider} is not a bean-defining annotation
   * for Arc itself — Quarkus REST's deployment processor adds every provider of the index as an
   * additional bean with that default scope, which this reproduces as the same annotation
   * transformation. Leaving a provider out is how a test models an application without Quarkus REST
   * (a headless application, whose request filter does not exist).
   *
   * @param beanClasses as {@link #boot(List)}
   * @param restProviders the provider classes Quarkus REST would register as {@code @Singleton}
   *     beans
   */
  static RealArcTestContainer bootWithRestProviders(
      List<Class<?>> beanClasses, List<Class<?>> restProviders) throws Exception {
    return boot(beanClasses, null, restProviders, Set.of(), false, Set.of());
  }

  /**
   * As {@link #boot(List)}, but the classes of {@code isolatedPackages} are defined by the
   * deployment classloader itself instead of being delegated to the test classpath — the way a
   * Quarkus application loads a library bean together with the bean class Arc generates for it.
   *
   * <p>Needed for a third-party bean with <em>package-private</em> injection points, such as the
   * SmallRye Health reporter: the generated {@code _Bean} class sets those fields directly, which
   * the JVM only allows inside one runtime package, i.e. within one classloader. Delegated to the
   * test classpath, the bean class and its generated {@code _Bean} would sit in two runtime
   * packages and creating the bean would fail with an {@link IllegalAccessError}. A package is
   * isolated together with its subpackages: a library's classes reference each other across its
   * packages, and a class the test classpath defines next to one the deployment classloader defines
   * breaks the JVM's loader constraints as soon as a signature mentions both.
   *
   * @param beanClasses as {@link #boot(List)}
   * @param isolatedPackages the packages (with their subpackages) whose classes the deployment
   *     classloader defines
   */
  static RealArcTestContainer bootIsolatingPackages(
      List<Class<?>> beanClasses, Set<String> isolatedPackages) throws Exception {
    return boot(beanClasses, null, List.of(), isolatedPackages, false, Set.of());
  }

  /**
   * As {@link #bootWithRestProviders}, but with Arc's build-time <strong>removal of unused
   * beans</strong> switched on — the Quarkus default ({@code quarkus.arc.remove-unused-beans=all}),
   * which every other boot of this harness switches off.
   *
   * <p>Arc then drops every bean that no injection point resolves to, that no {@code Instance} or
   * {@code @All} injection point matches, and that is neither named nor declares an observer. A
   * lookup through the {@code BeanManager} at runtime is invisible to that analysis, so a bean the
   * framework only finds that way is gone from a real Quarkus application although every boot with
   * removal switched off still sees it.
   *
   * <p>A real application keeps the classes it uses directly — a REST resource, which Quarkus REST
   * registers as unremovable, injecting the {@code CommandBus} — and those keep what they inject.
   * {@code entryPoints} models them: each is indexed as a bean class and excluded from removal, as
   * are the {@code restProviders}, which Quarkus REST registers as unremovable too.
   *
   * @param beanClasses as {@link #boot(List)}
   * @param restProviders as {@link #bootWithRestProviders}
   * @param entryPoints bean classes the deployment keeps whether or not anything injects them
   */
  static RealArcTestContainer bootRemovingUnusedBeans(
      List<Class<?>> beanClasses, List<Class<?>> restProviders, Set<Class<?>> entryPoints)
      throws Exception {
    List<Class<?>> allBeanClasses = new ArrayList<>(beanClasses);
    for (Class<?> entryPoint : entryPoints) {
      if (!allBeanClasses.contains(entryPoint)) {
        allBeanClasses.add(entryPoint);
      }
    }
    Set<Class<?>> unremovable = new HashSet<>(entryPoints);
    unremovable.addAll(restProviders);
    return boot(allBeanClasses, null, restProviders, Set.of(), true, unremovable);
  }

  private static RealArcTestContainer boot(
      List<Class<?>> beanClasses,
      Map<String, String> buildTimeProperties,
      List<Class<?>> restProviders,
      Set<String> isolatedPackages,
      boolean removeUnusedBeans,
      Set<Class<?>> unremovable)
      throws Exception {
    Path genDir = Files.createTempDirectory("arc-gen");

    List<Class<?>> allClasses = new ArrayList<>(beanClasses);
    allClasses.addAll(restProviders);
    Set<DotName> singletonByDefault = new HashSet<>();
    for (Class<?> provider : restProviders) {
      singletonByDefault.add(DotName.createSimple(provider.getName()));
    }

    Indexer indexer = new Indexer();
    Set<String> alreadyIndexed = new HashSet<>();
    for (Class<?> c : allClasses) {
      index(indexer, c, alreadyIndexed);
      for (Method m : c.getDeclaredMethods()) {
        if (m.isAnnotationPresent(jakarta.enterprise.inject.Produces.class)) {
          // Force JDK return types into the index too. Arc rejects a producer whose
          // return type is missing ("Producer method return type not found in index"), and the
          // blanket java.* skip below made a producer of a JDK type — java.net.http.HttpClient,
          // exactly the ubiquitous kind an application also declares — impossible to exercise here.
          index(indexer, m.getReturnType(), alreadyIndexed, true);
        }
      }
    }
    Index appIndex = indexer.complete();
    IndexView beanArchiveIndex = BeanArchives.buildImmutableBeanArchiveIndex(appIndex);

    List<GeneratedResource> generated = new ArrayList<>();
    ResourceOutput output =
        resource -> {
          // A JAVA_CLASS resource's name is the internal class name with no extension; a
          // SERVICE_PROVIDER resource's name is the bare service-interface FQN that must live under
          // META-INF/services/. Map both to their on-disk classpath locations so the deployment
          // classloader (and Arc's ServiceLoader) can find them.
          String path;
          if (resource.getType() == ResourceOutput.Resource.Type.JAVA_CLASS) {
            path = resource.getName() + ".class";
          } else if (resource.getType() == ResourceOutput.Resource.Type.SERVICE_PROVIDER) {
            path = "META-INF/services/" + resource.getName();
          } else {
            path = resource.getName();
          }
          generated.add(new GeneratedResource(path, resource.getData()));
        };

    BeanProcessor.Builder processorBuilder =
        BeanProcessor.builder()
            // No hyphens: this name becomes part of the generated ComponentsProvider class name,
            // and java.util.ServiceLoader rejects a provider class name that is not a legal Java
            // identifier.
            .setName("streamruneQuarkusTest")
            .setImmutableBeanArchiveIndex(beanArchiveIndex)
            .setApplicationIndex(appIndex)
            .setOutput(output)
            // Unless asked otherwise, keep every default bean the producers declare even if this
            // minimal deployment has no consumer for it — the test asserts on their
            // presence/uniqueness directly.
            .setRemoveUnusedBeans(removeUnusedBeans)
            .setTransformUnproxyableClasses(true);
    if (!unremovable.isEmpty()) {
      Set<DotName> unremovableNames = new HashSet<>();
      for (Class<?> c : unremovable) {
        unremovableNames.add(DotName.createSimple(c.getName()));
      }
      processorBuilder.addRemovalExclusion(
          bean -> bean.isClassBean() && unremovableNames.contains(bean.getBeanClass()));
    }
    if (buildTimeProperties != null) {
      processorBuilder.addAnnotationTransformation(buildTimeConditions(buildTimeProperties));
    }
    if (!singletonByDefault.isEmpty()) {
      processorBuilder.addAnnotationTransformation(
          ctx -> {
            if (ctx.declaration().kind() == AnnotationTarget.Kind.CLASS
                && singletonByDefault.contains(ctx.declaration().asClass().name())) {
              ctx.add(jakarta.inject.Singleton.class);
            }
          });
    }
    BeanProcessor processor = processorBuilder.build();
    processor.process();

    // Write the generated Arc classes + META-INF/services/...ComponentsProvider into genDir.
    for (GeneratedResource r : generated) {
      Path target = genDir.resolve(r.name());
      Files.createDirectories(target.getParent());
      Files.write(target, r.data());
    }

    URLClassLoader deploymentClassLoader =
        new DeploymentClassLoader(
            new URL[] {genDir.toUri().toURL()},
            RealArcTestContainer.class.getClassLoader(),
            isolatedPackages);

    ClassLoader previousTccl = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(deploymentClassLoader);
    try {
      ArcContainer container = Arc.initialize();
      return new RealArcTestContainer(deploymentClassLoader, previousTccl, container);
    } catch (Throwable t) {
      // Throwable, not RuntimeException: a deployment problem can surface as an Error (e.g. an
      // IllegalAccessError from a bean type the generated Arc class cannot see). Leaving the TCCL
      // pointing at a dead deployment classloader poisons every later container boot in this JVM,
      // so one broken test would cascade into unrelated failures across the whole test class.
      Thread.currentThread().setContextClassLoader(previousTccl);
      deploymentClassLoader.close();
      throw t;
    }
  }

  /**
   * Indexes {@code c} plus its superclass/interface closure, skipping primitives, arrays, and JDK
   * types (Arc's built-in index covers those). Idempotent via {@code alreadyIndexed}.
   */
  private static void index(Indexer indexer, Class<?> c, Set<String> alreadyIndexed)
      throws IOException {
    index(indexer, c, alreadyIndexed, false);
  }

  private static void index(
      Indexer indexer, Class<?> c, Set<String> alreadyIndexed, boolean forceJdkType)
      throws IOException {
    if (c == null || c.isPrimitive() || c.isArray()) {
      return;
    }
    boolean jdkType = c.getName().startsWith("java.") || c.getName().startsWith("jakarta.");
    if (jdkType && !forceJdkType) {
      return;
    }
    if (!alreadyIndexed.add(c.getName())) {
      return;
    }
    String path = c.getName().replace('.', '/') + ".class";
    // A JDK class has no class loader — read its bytes from its own module instead.
    try (var in =
        c.getClassLoader() == null
            ? c.getModule().getResourceAsStream(path)
            : c.getClassLoader().getResourceAsStream(path)) {
      if (in == null) {
        throw new IOException("Cannot find class bytes for " + c.getName());
      }
      indexer.index(in);
    }
    index(indexer, c.getSuperclass(), alreadyIndexed);
    for (Class<?> i : c.getInterfaces()) {
      index(indexer, i, alreadyIndexed);
    }
  }

  @Override
  public void close() {
    try {
      Arc.shutdown();
    } finally {
      Thread.currentThread().setContextClassLoader(previousTccl);
      try {
        deploymentClassLoader.close();
      } catch (IOException _) {
        // best-effort cleanup
      }
    }
  }

  private static final DotName IF_BUILD_PROPERTY =
      DotName.createSimple("io.quarkus.arc.properties.IfBuildProperty");
  private static final DotName IF_BUILD_PROPERTY_LIST =
      DotName.createSimple("io.quarkus.arc.properties.IfBuildProperty$List");

  /**
   * The transformation {@code BuildTimeEnabledProcessor#conditionTransformer} installs, restricted
   * to {@code @IfBuildProperty} (the only build-time condition this module uses): a declaration
   * whose conditions do not all hold is vetoed — as a whole class, or as a producer.
   *
   * <p>The CLASS/non-CLASS split is the whole point of the fix it guards. Arc consults
   * {@code @VetoedProducer} only where it collects {@code @Produces} methods and fields; the loop
   * that collects {@code @Disposes} methods tests for {@code @Disposes} and nothing else. A vetoed
   * <em>class</em>, by contrast, never becomes a bean class at all, so neither its producers nor
   * its disposers are ever looked at.
   */
  private static AnnotationTransformation buildTimeConditions(Map<String, String> properties) {
    return ctx -> {
      List<AnnotationInstance> conditions = new ArrayList<>();
      for (AnnotationInstance annotation : ctx.annotations()) {
        if (annotation.name().equals(IF_BUILD_PROPERTY)) {
          conditions.add(annotation);
        } else if (annotation.name().equals(IF_BUILD_PROPERTY_LIST)) {
          conditions.addAll(List.of(annotation.value().asNestedArray()));
        }
      }
      if (conditions.isEmpty() || conditions.stream().allMatch(c -> holds(c, properties))) {
        return;
      }
      if (ctx.declaration().kind() == AnnotationTarget.Kind.CLASS) {
        ctx.add(jakarta.enterprise.inject.Vetoed.class);
      } else {
        ctx.add(io.quarkus.arc.VetoedProducer.class);
      }
    };
  }

  private static boolean holds(AnnotationInstance condition, Map<String, String> properties) {
    String value = properties.get(condition.value("name").asString());
    if (value == null) {
      var enableIfMissing = condition.value("enableIfMissing");
      return enableIfMissing != null && enableIfMissing.asBoolean();
    }
    return condition.value("stringValue").asString().equals(value);
  }

  private record GeneratedResource(String name, byte[] data) {}

  /**
   * Parent-first, except for the classes of the isolated packages and their subpackages: those are
   * looked up in the generated classes first and otherwise defined here from the parent's class
   * bytes, so a bean class and its generated {@code _Bean} share one runtime package.
   */
  private static final class DeploymentClassLoader extends URLClassLoader {

    private final Set<String> isolatedPackages;

    DeploymentClassLoader(URL[] urls, ClassLoader parent, Set<String> isolatedPackages) {
      super(urls, parent);
      this.isolatedPackages = isolatedPackages;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (isolatedPackages.stream().noneMatch(p -> name.startsWith(p + "."))) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          loaded = defineIsolated(name);
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }

    private Class<?> defineIsolated(String name) throws ClassNotFoundException {
      try {
        return findClass(name);
      } catch (ClassNotFoundException notGenerated) {
        String path = name.replace('.', '/') + ".class";
        try (var in = getParent().getResourceAsStream(path)) {
          if (in == null) {
            throw notGenerated;
          }
          byte[] bytes = in.readAllBytes();
          return defineClass(name, bytes, 0, bytes.length);
        } catch (IOException e) {
          throw new ClassNotFoundException(name, e);
        }
      }
    }
  }
}
